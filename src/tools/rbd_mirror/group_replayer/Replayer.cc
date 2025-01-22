// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Replayer.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "include/stringify.h"
#include "common/Timer.h"
#include "cls/rbd/cls_rbd_client.h"
#include "json_spirit/json_spirit.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::Replayer: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    librados::IoCtx &local_io_ctx,
    librados::IoCtx &remote_io_ctx,
    const std::string &global_group_id,
    const std::string& local_mirror_uuid,
    const std::string& remote_mirror_uuid,
    InstanceWatcher<I> *instance_watcher,
    MirrorStatusUpdater<I> *local_status_updater,
    MirrorStatusUpdater<I> *remote_status_updater,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache* pool_meta_cache,
    std::string local_group_id,
    std::string remote_group_id,
    GroupCtx *local_group_ctx)
  : m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_global_group_id(global_group_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid),
    m_instance_watcher(instance_watcher),
    m_local_status_updater(local_status_updater),
    m_remote_status_updater(remote_status_updater),
    m_cache_manager_handler(cache_manager_handler),
    m_pool_meta_cache(pool_meta_cache),
    m_local_group_id(local_group_id),
    m_remote_group_id(remote_group_id),
    m_local_group_ctx(local_group_ctx),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::group_replayer::Replayer", this))) {
  dout(10) << m_global_group_id <<  dendl;
}

template <typename I>
Replayer<I>::~Replayer() {
  dout(10) << m_global_group_id << dendl;

  ceph_assert(m_state == STATE_COMPLETE);
}

template <typename I>
bool Replayer<I>::is_replay_interrupted() {
  std::unique_lock locker{m_lock};
  return is_replay_interrupted(&locker);
}

template <typename I>
bool Replayer<I>::is_replay_interrupted(std::unique_lock<ceph::mutex>* locker) {
  if (m_state == STATE_COMPLETE) {
    locker->unlock();

    return true;
  }

  return false;
}

template <typename I>
void Replayer<I>::notify_group_listener_stop() {
  dout(10) << dendl;

  Context *ctx = new LambdaContext([this](int) {
      m_local_group_ctx->listener->stop();
      });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
int Replayer<I>::local_group_image_list_by_id(
    std::vector<cls::rbd::GroupImageStatus> *image_ids) {
  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_id);

  dout(10) << "local_group_id=" << m_local_group_id << dendl;
  image_ids->clear();

  int r = 0;
  const int max_read = 1024;
  cls::rbd::GroupImageSpec start_last;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;

    r = librbd::cls_client::group_image_list(&m_local_io_ctx, group_header_oid,
                                             start_last, max_read,
                                             &image_ids_page);

    if (r < 0) {
      derr << "error reading image list from local group: "
           << cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids->insert(image_ids->end(), image_ids_page.begin(),
                      image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  return 0;
}


template <typename I>
bool Replayer<I>::is_resync_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_id);
  std::string value;
  int r = librbd::cls_client::metadata_get(&m_local_io_ctx, group_header_oid,
                                           RBD_GROUP_RESYNC, &value);
  if (r < 0 && r != -ENOENT) {
    derr << "failed reading metadata: " << cpp_strerror(r) << dendl;
  } else if (r == 0) {
    return true;
  }

  return false;
}

template <typename I>
bool Replayer<I>::is_rename_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::string remote_group_name;
  int r = librbd::cls_client::dir_get_name(&m_remote_io_ctx,
                                           RBD_GROUP_DIRECTORY,
                                           m_remote_group_id,
                                           &remote_group_name);
  if (r < 0) {
    derr << "failed to retrieve remote group name: "
         << cpp_strerror(r) << dendl;
    return false;
  }

  if (m_local_group_ctx && m_local_group_ctx->name != remote_group_name) {
    return true;
  }

  return false;
}

template <typename I>
void Replayer<I>::init(Context* on_finish) {
  dout(10) << "m_global_group_id=" << m_global_group_id << dendl;

  ceph_assert(m_state == STATE_INIT);

  RemotePoolMeta remote_pool_meta;
  int r = m_pool_meta_cache->get_remote_pool_meta(
    m_remote_io_ctx.get_id(), &remote_pool_meta);
  if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
    derr << "failed to retrieve mirror peer uuid from remote pool" << dendl;
    m_state = STATE_COMPLETE;
    m_threads->work_queue->queue(on_finish, r);
    return;
  }

  m_remote_mirror_peer_uuid = remote_pool_meta.mirror_peer_uuid;
  dout(10) << "remote_mirror_peer_uuid=" << m_remote_mirror_peer_uuid << dendl;

  on_finish->complete(0);
  load_local_group_snapshots();
}

template <typename I>
void Replayer<I>::load_local_group_snapshots() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }
  if (m_state == STATE_INIT) {
    m_state = STATE_REPLAYING;
  } else {
    sleep(1);
  }

  if (m_stop_requested) {
    return;
  } else if (is_resync_requested()) {
    m_stop_requested = true;
    dout(10) << "local group resync requested" << dendl;
    // send stop for Group Replayer
    stop_image_replayers();
    return;
  } else if (is_rename_requested()) {
    m_stop_requested = true;
    dout(10) << "remote group rename requested" << dendl;
    // send stop for Group Replayer
    stop_image_replayers();
    return;
  }

  m_local_group_snaps.clear();
  auto ctx = create_context_callback<
      Replayer<I>,
      &Replayer<I>::handle_load_local_group_snapshots>(this);

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_local_io_ctx,
      m_local_group_id, true, true, &m_local_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_local_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error listing local mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    load_local_group_snapshots();
    return;
  }

  std::unique_lock locker{m_lock};
  for (auto it = m_local_group_snaps.rbegin();
       it != m_local_group_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }
    // Get the first non-primary mirror  it finds from the end. Why from the end?
    if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      break;
    } else if (ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
      m_stop_requested = true;
      // stop group replayer
      stop_image_replayers();
      locker.unlock();
      return;
    }
    // this is primary, IDLE the group replayer
    m_state = STATE_IDLE;
    m_create_group_snap = &(*it);
    if (!m_last_local_group_snap) {
      m_last_local_group_snap = m_create_group_snap;
    } else if (m_last_local_group_snap->id == m_create_group_snap->id) {
      sleep(1);
      locker.unlock();
      load_local_group_snapshots();
      return;
    }
    m_last_local_group_snap = m_create_group_snap;
    stop_image_replayers();
    locker.unlock();
    return;
  }
  locker.unlock();

  load_remote_group_snapshots();
}

template <typename I>
void Replayer<I>::load_remote_group_snapshots() {
  dout(10) << "m_remote_group_id=" << m_remote_group_id << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }
  m_remote_group_snaps.clear();
  auto ctx = new LambdaContext(
    [this] (int r) {
      handle_load_remote_group_snapshots(r);
  });

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_remote_io_ctx,
      m_remote_group_id, true, true, &m_remote_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_remote_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error listing remote mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    load_remote_group_snapshots();
    return;
  }

  if (!m_local_group_snaps.empty()) {
    auto last_local_snap = m_local_group_snaps.rbegin();
    if (m_image_replayers.empty()) {
      dout(10) << "PK: Here" << dendl;
      for (auto remote_snap = m_remote_group_snaps.begin();
          remote_snap != m_remote_group_snaps.end(); ++remote_snap) {
        dout(10) << "PK: Here XYZ1: " << remote_snap->id << dendl;
        if (last_local_snap->id == remote_snap->id) {
          m_create_group_snap = &(*remote_snap);
          if (remote_snap->snaps.size() != 0) {
            create_replayers();
            return;
          }
          break;
        }
      }
    }
    if (last_local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      // do not forward until previous local group snapshot is COMPLETE
      validate_image_snaps_sync_complete(last_local_snap->id);
      load_local_group_snapshots();
      return;
    }

    auto last_local_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &last_local_snap->snapshot_namespace);
    if (last_local_snap_ns &&
        last_local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED &&
        !m_remote_group_snaps.empty()) {
      auto last_remote_snap = m_remote_group_snaps.rbegin();
      if (last_local_snap->id == last_remote_snap->id) {
        m_stop_requested = true;
        std::unique_lock locker{m_lock};
        stop_image_replayers();
        return;
      }
    }
  }

  scan_for_unsynced_group_snapshots();
}

template <typename I>
void Replayer<I>::validate_image_snaps_sync_complete(
    const std::string &remote_group_snap_id) {
  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }
  // 1. get group membership
  // 2. get snap list of each image and check any image snap has the group
  // snapid and is set to complete. If yes call complete
  dout(10) << "group snap_id: " << remote_group_snap_id << dendl;

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itr == m_remote_group_snaps.end()) {
    return;
  }

  // TODO: take care about image addition scenario to group
  std::vector<cls::rbd::GroupImageStatus> local_images;
  int r = local_group_image_list_by_id(&local_images);
  if (r < 0) {
    derr << "failed group image list: " << cpp_strerror(r) << dendl;
    return;
  }

  // Check the image snapshot count in the remote group snap
  // FIXME: this Hurts the resync
  if (itr->snaps.size() > local_images.size()) {
    dout(20) << "group membership changed, will retry later, remote snapshot images count: "
         << itr->snaps.size() << ", local group images count: "
         << local_images.size() << dendl;
    // need restart of group replayer to pull the new images,
    // which will start/stop fresh set of Imagereplayers
    m_stop_requested = true;
    stop_image_replayers();
    return;
    // return; when above cond is != Why commentting this? removing an image is leading to
    //incomplete snap on remote
  }

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itl == m_local_group_snaps.end()) {
    return;
  }
  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      itl->snapshot_namespace);
  if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
    C_SaferCond *ctx = new C_SaferCond;
    regular_snapshot_complete(remote_group_snap_id, ctx);
    ctx->wait();
    return;
  }

  // FIXME: get the latest image spec added to a different pool,
  // for now searching only in the local pool.

  // 1. Get remote image_id
  // 2. Translate to local image id
  // 3. get the image spec

  if (itr->snaps.size() == 0) {
    dout(5) << "Image list is empty!!" << dendl;
    C_SaferCond *ctx = new C_SaferCond;
    mirror_snapshot_complete(remote_group_snap_id, nullptr, ctx);
    ctx->wait();
    return;
  }
  for (auto& image : local_images) {
    std::string image_header_oid = librbd::util::header_name(
        image.spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
        image_header_oid, &snapc);
    if (r < 0) {
      derr << "get snap context failed: " << cpp_strerror(r) << dendl;
      return;
    }

    // stored in reverse order
    for (auto snap_id : snapc.snaps) {
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << snap_id
          << ", : " << cpp_strerror(r) << dendl;
        return;
      }
      auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
          &snap_info.snapshot_namespace);
      if (!mirror_ns) {
        continue;
      }
      // Makesure the image snapshot is COMPLETE
      if (mirror_ns->group_snap_id == remote_group_snap_id && mirror_ns->complete) {
        cls::rbd::ImageSnapshotSpec snap_spec;
        snap_spec.pool = image.spec.pool_id;
        snap_spec.image_id = image.spec.image_id;
        snap_spec.snap_id = snap_info.id;
        auto it = std::find_if(
          itl->snaps.begin(), itl->snaps.end(),
          [&snap_spec](const cls::rbd::ImageSnapshotSpec &s) {
            return snap_spec.pool == s.pool && snap_spec.image_id == s.image_id;
          });
        // Send to only if the image spec absent in INCOMPLETE group Snapshot
        if (it == itl->snaps.end()) {
          C_SaferCond *ctx = new C_SaferCond;
          mirror_snapshot_complete(remote_group_snap_id, &snap_spec, ctx);
          ctx->wait();
        }
        continue;
      } else {
        dout(10) << "remote group snap id: " << remote_group_snap_id
                 << ", local reflected in the image snap: "
                 << mirror_ns->group_snap_id << dendl;
      }
    }
  }
}

template <typename I>
bool Replayer<I>::is_membership_changed(cls::rbd::GroupSnapshot next_remote_snap) {
  if (next_remote_snap.snaps.size() != m_image_replayer_index.size()) {
    return true;
  }

  bool found;
  for (auto &[p, _] : m_image_replayer_index) {
    auto &remote_image_id = p.second;
    found = false;
    for (auto &snap : next_remote_snap.snaps) {
      if (snap.image_id == remote_image_id) {
        found = true;
        break;
      }
    }
    if (!found) {
      return true;
    }
  }

  return false;
}

template <typename I>
void Replayer<I>::scan_for_unsynced_group_snapshots() {
  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }
  dout(10) << dendl;

  bool found = false;
  bool syncs_upto_date = false;
  if (m_remote_group_snaps.empty()) {
    goto out;
  }

  // check if we have a matching snap on remote to start with.
  for (auto local_snap = m_local_group_snaps.rbegin();
       local_snap != m_local_group_snaps.rend(); ++local_snap) {
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        local_snap->snapshot_namespace);
    auto local_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &local_snap->snapshot_namespace);
    auto next_remote_snap = m_remote_group_snaps.end();
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER ||
        (local_snap_ns && (local_snap_ns->is_non_primary() ||
        local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED))) {
      for (auto remote_snap = m_remote_group_snaps.begin();
           remote_snap != m_remote_group_snaps.end(); ++remote_snap) {
        if (local_snap->id == remote_snap->id) {
          next_remote_snap = std::next(remote_snap);
          found = true;
          break;
        }
      }
    }
    if (found && next_remote_snap == m_remote_group_snaps.end()) {
      syncs_upto_date = true;
      break;
    }
    if (next_remote_snap != m_remote_group_snaps.end()) {
      auto id = next_remote_snap->id;
      auto itl = std::find_if(
          m_local_group_snaps.begin(), m_local_group_snaps.end(),
          [id](const cls::rbd::GroupSnapshot &s) {
          return s.id == id;
          });
      if (found && itl == m_local_group_snaps.end()) {
        m_create_group_snap = &(*next_remote_snap);
        dout(10) << "PK: 1 " << dendl;
        if (is_membership_changed(*m_create_group_snap)) {
          stop_image_replayers();
          locker.unlock();
        } else {
          try_create_group_snapshot(*m_create_group_snap, locker);
          locker.unlock();
          load_local_group_snapshots();
        }
        return;
      }
    }
    found = false;
  }
  if (!syncs_upto_date) {
    dout(10) << "none of the local snaps match remote" << dendl;
    auto remote_snap = m_remote_group_snaps.rbegin();
    for(; remote_snap != m_remote_group_snaps.rend(); ++remote_snap) {
      auto prev_remote_snap = std::next(remote_snap);
      if (prev_remote_snap == m_remote_group_snaps.rend()) {
        break;
      }
      auto snap_type = cls::rbd::get_group_snap_namespace_type(
          prev_remote_snap->snapshot_namespace);
      if (snap_type != cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
        continue;
      }
      auto prev_remote_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
          &prev_remote_snap->snapshot_namespace);
      if (prev_remote_snap_ns && prev_remote_snap_ns->is_demoted()) {
        break;
      }
    }
    auto id = remote_snap->id;
    auto itl = std::find_if(
        m_local_group_snaps.begin(), m_local_group_snaps.end(),
        [id](const cls::rbd::GroupSnapshot &s) {
        return s.id == id;
        });
    if (remote_snap != m_remote_group_snaps.rend() &&
        itl == m_local_group_snaps.end()) {
      m_create_group_snap = &(*remote_snap);
      dout(10) << "PK: 2 " << dendl;
        if (is_membership_changed(*m_create_group_snap)) {
          stop_image_replayers();
          locker.unlock();
        } else {
          try_create_group_snapshot(*m_create_group_snap, locker);
          locker.unlock();
          load_local_group_snapshots();
        }
      return;
    }
  }

out:
  // At this point all group snapshots have been synced, but we keep poll
  if (m_stop_requested) {
    // stop group replayer
    dout(10) << "PK: 3 " << dendl;
    stop_image_replayers();
  }
  locker.unlock();
  load_local_group_snapshots();
}

template <typename I>
std::string Replayer<I>::prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id,
    const std::string &snap_id) {
  dout(5) << "global_group_id: " << global_group_id
          << ", snap_id: " << snap_id << dendl;
  std::stringstream ind_snap_name_stream;
  ind_snap_name_stream << ".mirror.non-primary."
                       << global_group_id << "." << snap_id;
  return ind_snap_name_stream.str();
}

template <typename I>
void Replayer<I>::try_create_group_snapshot(cls::rbd::GroupSnapshot snap,
                                            std::unique_lock<ceph::mutex> &locker) {
  dout(10) << snap.id << dendl;
  if (is_replay_interrupted(&locker)) {
    return;
  }
  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      snap.snapshot_namespace);
  if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
    auto snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
    if (snap_ns->is_non_primary()) {
      dout(10) << "remote group snapshot: " << snap.id << "is non primary"
               << dendl;
      return;
    }
    auto snap_state =
      snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY ?
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY :
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;
    C_SaferCond *ctx = new C_SaferCond;
    create_mirror_snapshot(snap.id, snap_state, ctx);
    ctx->wait();
  } else if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
    bool found = false;
    auto next_remote_snap = m_remote_group_snaps.end();
    for (auto remote_snap = m_remote_group_snaps.begin();
        remote_snap != m_remote_group_snaps.end(); ++remote_snap) {
      next_remote_snap = std::next(remote_snap);
      if (remote_snap->id == snap.id) {
        found = true;
      }
      if (!found) {
        continue;
      }
      if (next_remote_snap == m_remote_group_snaps.end()) {
        return; // done
      }
      auto st = cls::rbd::get_group_snap_namespace_type(
          next_remote_snap->snapshot_namespace);
      if (st == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
        continue;
      } else if (next_remote_snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
        return; //wait and try later
      } else {
        break; // We have a mirror group snapshot, we can copy regular group snap
      }
    }
    if (next_remote_snap == m_remote_group_snaps.end()) {
      return;
    }
    dout(10) << "found regular snap, snap name: " << snap.name
             << ", remote group snap id: " << snap.id << dendl;
    C_SaferCond *ctx = new C_SaferCond;
    create_regular_snapshot(snap.name, snap.id, ctx);
    ctx->wait();
  }
}

template <typename I>
void Replayer<I>::create_mirror_snapshot(
    const std::string &remote_group_snap_id,
    const cls::rbd::MirrorSnapshotState &snap_state,
    Context *on_finish) {
  dout(10) << remote_group_snap_id << dendl;

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itl != m_local_group_snaps.end() &&
      itl->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    dout(20) << "group snapshot: " << remote_group_snap_id << " already exists"
             << dendl;
    on_finish->complete(0);
    return;
  }

  auto requests_it = m_create_snap_requests.find(remote_group_snap_id);
  if (requests_it == m_create_snap_requests.end()) {
    requests_it = m_create_snap_requests.insert(
        {remote_group_snap_id, {}}).first;
    cls::rbd::GroupSnapshot local_snap =
      {remote_group_snap_id,
       cls::rbd::GroupSnapshotNamespaceMirror{
         snap_state, {}, m_remote_mirror_uuid, remote_group_snap_id},
       {}, cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
    local_snap.name = prepare_non_primary_mirror_snap_name(m_global_group_id,
        remote_group_snap_id);
    m_local_group_snaps.push_back(local_snap);

    auto comp = create_rados_callback(
      new LambdaContext([this, remote_group_snap_id, on_finish](int r) {
        handle_create_mirror_snapshot(r, remote_group_snap_id, on_finish);
      }));

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, local_snap);
    int r = m_local_io_ctx.aio_operate(
        librbd::util::group_header_name(m_local_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();
  } else {
    on_finish->complete(0);
  }
}

template <typename I>
void Replayer<I>::handle_create_mirror_snapshot(
    int r, const std::string &remote_group_snap_id, Context *on_finish) {
  dout(10) << remote_group_snap_id << ", r=" << r << dendl;

  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::mirror_snapshot_complete(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec *spec,
    Context *on_finish) {

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  ceph_assert(itr != m_remote_group_snaps.end());
  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itr->snaps.size() != 0) {
    // update the group snap with snap spec
    itl->snaps.push_back(*spec);
  }

  if (itr->snaps.size() == itl->snaps.size()) {
    m_create_snap_requests.erase(remote_group_snap_id);
    itl->state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  }

  dout(10) << "local group snap info: "
           << "id: " << itl->id
           << ", name: " << itl->name
           << ", state: " << itl->state
           << ", snaps.size: " << itl->snaps.size()
           << dendl;
  auto comp = create_rados_callback(
    new LambdaContext([this, remote_group_snap_id, on_finish](int r) {
      handle_mirror_snapshot_complete(r, remote_group_snap_id, on_finish);
    }));

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, *itl);
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_mirror_snapshot_complete(
    int r, const std::string &remote_group_snap_id, Context *on_finish) {
  dout(10) << remote_group_snap_id << ", r=" << r << dendl;

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itl->state !=
      cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    on_finish->complete(0);
    return;
  }

  // remove mirror_peer_uuids from remote snap
  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  ceph_assert(itr != m_remote_group_snaps.end());
  auto rns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &itr->snapshot_namespace);
  if (rns != nullptr) {
    rns->mirror_peer_uuids.clear();
    auto comp = create_rados_callback(
        new LambdaContext([this](int r) {
          dout(10) << "nothing" << dendl; //improve later move this to peer
                                          //remove funtion and a handle
        }));

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, *itr);
    int r = m_remote_io_ctx.aio_operate(
        librbd::util::group_header_name(m_remote_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }
  unlink_group_snapshots(remote_group_snap_id, on_finish);
}

template <typename I>
void Replayer<I>::unlink_group_snapshots(
    const std::string &remote_group_snap_id, Context *on_finish) {
  if (m_image_replayers.empty()) {
    on_finish->complete(0);
    return;
  }
  int r;
  for (auto &group_snap : m_local_group_snaps) {
    if (group_snap.id == remote_group_snap_id) {
      break;
    }
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        group_snap.snapshot_namespace);
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
      bool unlink_user_snap = true;
      for (auto &remote_snap : m_remote_group_snaps) {
        if (remote_snap.name == group_snap.name) {
          unlink_user_snap = false;
          break;
        }
      }
      if (!unlink_user_snap) {
        continue;
      }
      dout(10) << "unlinking regular group snap in-progress: "
               << group_snap.name << ", with id: " << group_snap.id << dendl;
    }
    dout(10) << "attempting to unlink image snaps from group snap: "
             << group_snap.id << dendl;
    bool retain = false;
    for (auto &spec : group_snap.snaps) {
      retain = true;
      std::string image_header_oid = librbd::util::header_name(spec.image_id);
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          spec.snap_id, &snap_info);
      if (r == -ENOENT) {
        continue;
      } else if (r < 0) {
        derr << "failed getting snap info for snap id: " << spec.snap_id
             << ", : " << cpp_strerror(r) << dendl;
      }
      for (auto it = m_image_replayers.begin();
           it != m_image_replayers.end(); ++it) {
        auto image_replayer = it->second;
        if (!image_replayer) {
          continue;
        }
        auto local_image_id = image_replayer->get_local_image_id();
        if (local_image_id.empty()) {
          continue;
        }
        if (local_image_id != spec.image_id) {
          continue;
        }
        dout(10) << "pruning: " << spec.snap_id << dendl;
        image_replayer->prune_snapshot(spec.snap_id);
        retain = false;
        break;
      }
      if (retain) {
        remove_image_snapshot(spec.image_id, spec.snap_id, spec.pool);
        continue;
      }
    }
    dout(10) << "all image snaps are pruned, finally unlinking group snap: "
             << group_snap.id << dendl;
    r = librbd::cls_client::group_snap_remove(&m_local_io_ctx,
        librbd::util::group_header_name(m_local_group_id), group_snap.id);
    if (r < 0) {
      derr << "failed to remove group snapshot : "
           << group_snap.id << " : " << cpp_strerror(r) << dendl;
    }
  }

  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::remove_image_snapshot(std::string image_id,
                                        uint64_t snap_id, int64_t pool_id) {
  dout(10) << snap_id << dendl;

  int r = -ENOENT;
  std::string image_header_oid = librbd::util::header_name(image_id);
  cls::rbd::SnapshotInfo snap_info;
  r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
      snap_id, &snap_info);
  if (r < 0) {
    derr << "failed getting snap info for snap id: " << snap_id
      << ", : " << cpp_strerror(r) << dendl;
    return;
  }
  auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &snap_info.snapshot_namespace);
  if (mirror_ns == nullptr) {
    derr << "not mirror snapshot (snap_id=" << snap_id << ")" << dendl;
    return;
  }
  librbd::ImageCtx* ictx = I::create("", image_id.c_str(),
                                                     nullptr, m_local_io_ctx,
                                                     false);
  // FIXME: the open blocks indefinitely, and the snap_remove fails with ENOENT
  /* C_SaferCond* on_finish = new C_SaferCond;
  ictx->state->open(0, on_finish);
  on_finish->wait(); */
  ictx->operations->snap_remove(snap_info.snapshot_namespace,
                                snap_info.name.c_str());

}

template <typename I>
void Replayer<I>::create_regular_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    Context *on_finish) {
  dout(10) << remote_group_snap_id << dendl;
  librados::ObjectWriteOperation op;
  cls::rbd::GroupSnapshot group_snap{
    remote_group_snap_id, // keeping it same as remote group snap id
    cls::rbd::GroupSnapshotNamespaceUser{},
      remote_group_snap_name,
      cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};

  librbd::cls_client::group_snap_set(&op, group_snap);
  auto comp = create_rados_callback(
      new LambdaContext([this, on_finish](int r) {
        handle_create_regular_snapshot(r, on_finish);
      }));
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_create_regular_snapshot(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local non-primary group snapshot: "
         << cpp_strerror(r) << dendl;
  }
  on_finish->complete(0);

}

template <typename I>
void Replayer<I>::regular_snapshot_complete(
    const std::string &remote_group_snap_id,
    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itl == m_local_group_snaps.end()) {
    on_finish->complete(0);
    return;
  }

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  // each image will have one snapshot specific to group snap, and so for each
  // image get a ImageSnapshotSpec and prepare a vector
  // for image :: <images in that group> {
  //   * get snap whos name has group snap_id for that we can list snaps and
  //     filter with remote_group_snap_id
  //   * get its { pool_id, snap_id, image_id }
  // }
  // finally write to the object

  std::vector<cls::rbd::ImageSnapshotSpec> local_image_snap_specs;
  if (itr != m_remote_group_snaps.end()) {
    local_image_snap_specs.reserve(itr->snaps.size());
    std::vector<cls::rbd::GroupImageStatus> local_images;
    int r = local_group_image_list_by_id(&local_images);
    if (r < 0) {
      derr << "failed group image list: " << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }
    for (auto& image : local_images) {
      std::string image_header_oid = librbd::util::header_name(
          image.spec.image_id);
      ::SnapContext snapc;
      int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
          image_header_oid, &snapc);
      if (r < 0) {
        derr << "get snap context failed: " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }

      auto image_snap_name = ".group." + std::to_string(image.spec.pool_id) +
        "_" + m_remote_group_id + "_" + remote_group_snap_id;
      // stored in reverse order
      for (auto snap_id : snapc.snaps) {
        cls::rbd::SnapshotInfo snap_info;
        r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
            snap_id, &snap_info);
        if (r < 0) {
          derr << "failed getting snap info for snap id: " << snap_id
            << ", : " << cpp_strerror(r) << dendl;
          on_finish->complete(r);
          return;
        }

        // extract { pool_id, snap_id, image_id }
        if (snap_info.name == image_snap_name) {
          cls::rbd::ImageSnapshotSpec snap_spec;
          snap_spec.pool = image.spec.pool_id;
          snap_spec.image_id = image.spec.image_id;
          snap_spec.snap_id = snap_info.id;

          local_image_snap_specs.push_back(snap_spec);
        }
      }
    }
  }

  if (itr->snaps.size() == local_image_snap_specs.size()) {
    itl->snaps = local_image_snap_specs;
    itl->state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  }
  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, *itl);

  auto comp = create_rados_callback(
      new LambdaContext([this, on_finish](int r) {
        handle_regular_snapshot_complete(r, on_finish);
      }));
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_regular_snapshot_complete(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;
  std::unique_lock locker{m_lock};
  m_stop_requested = true;
  m_state = STATE_COMPLETE;
  stop_image_replayers();
  locker.unlock();
  if (on_finish) {
    on_finish->complete(0);
  }
  return;
}

template <typename I>
void Replayer<I>::create_replayers() {
  int r = 0;
  dout(10) << dendl;

  if (is_replay_interrupted()) {
    return;
  }
  dout(10) << dendl;

  if (m_stop_requested) {
    if (m_state != STATE_COMPLETE) {
      notify_group_listener_stop();
    }
    return;
  }

  m_snap_members.clear();
  m_image_replayers.clear();

  //if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
  //    m_remote_mirror_group_primary) {

    //bool found;
  if (m_state != STATE_IDLE) {
    for (auto &image_snap : m_create_group_snap->snaps) {
      cls::rbd::MirrorImage mirror_image;
      r = librbd::cls_client::mirror_image_get(&m_remote_io_ctx,
                                               image_snap.image_id,
                                               &mirror_image);
      if (r < 0) {
        derr << "mirror image get failed for: " << image_snap.image_id << " : "
             << cpp_strerror(r) << dendl;
      }
      m_snap_members[{image_snap.pool, mirror_image.global_image_id}] = image_snap.image_id;
    }

    for (auto &[p, image_id]: m_snap_members) {
      dout(10) << "members { pool_id: " << p.first
               << ", global_image_id: " << p.second
               << " }, image_id: " << image_id << dendl;
    }
    for (auto &[p, remote_image_id] : m_snap_members) {
      /* found = false;
      for (auto &[q, _] : m_image_replayer_index) {
        if (q.second == remote_image_id) {
          dout(10) <<  "PKX: matching: " << remote_image_id << " continue, no change to IR" << dendl;
          found = true;
          break;
        }
      }
      if (found) {
        continue;
      } */
      // Will fallback here for new entries only
      dout(10) <<  "spawning new IR for: " << remote_image_id << dendl;

      auto &remote_pool_id = p.first;
      auto &global_image_id = p.second;

      m_image_replayers.emplace_back(librados::IoCtx(), nullptr);
      auto &local_io_ctx = m_image_replayers.back().first;
      auto &image_replayer = m_image_replayers.back().second;

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_pool_id,
                                                  &remote_pool_meta);
      if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
        derr << "failed to retrieve mirror peer uuid from remote image pool"
             << dendl;
        r = -ENOENT;
        break;
      }

      librados::IoCtx remote_io_ctx;
      r = librbd::util::create_ioctx(m_remote_io_ctx, "remote image pool",
                                     remote_pool_id, {}, &remote_io_ctx);
      if (r < 0) {
        derr << "failed to open remote image pool " << remote_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      int64_t local_pool_id = librados::Rados(m_local_io_ctx).pool_lookup(
          remote_io_ctx.get_pool_name().c_str());

      LocalPoolMeta local_pool_meta;
      r = m_pool_meta_cache->get_local_pool_meta(local_pool_id,
                                                 &local_pool_meta);
      if (r < 0 || local_pool_meta.mirror_uuid.empty()) {
        if (r == 0 || r == -ENOENT) {
          r = -EINVAL;
        }
        derr << "failed to retrieve mirror uuid from local image pool" << dendl;
        break;
      }

      r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     local_pool_id, {}, &local_io_ctx);
      if (r < 0) {
        derr << "failed to open local image pool " << local_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      image_replayer = ImageReplayer<I>::create(
        local_io_ctx, m_local_group_ctx, local_pool_meta.mirror_uuid,
        global_image_id, m_threads, m_instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

      // TODO only a single peer is currently supported
      image_replayer->add_peer({local_pool_meta.mirror_uuid, remote_io_ctx,
                                remote_pool_meta, m_remote_status_updater});

      m_image_replayer_index[{remote_pool_id, remote_image_id}] = image_replayer;
    }
  //}
  // else if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
  //           m_local_mirror_group_primary) {
  } else {
    m_snap_members.clear();
    for (auto &image_snap : m_create_group_snap->snaps) {
      cls::rbd::MirrorImage mirror_image;
      r = librbd::cls_client::mirror_image_get(&m_local_io_ctx,
          image_snap.image_id,
          &mirror_image);
      if (r < 0) {
        derr << "mirror image get failed for: " << image_snap.image_id << " : "
          << cpp_strerror(r) << dendl;
      }
      m_snap_members[{image_snap.pool, mirror_image.global_image_id}] = image_snap.image_id;
    }
    for (auto &[p, local_image_id] : m_snap_members) {
      auto &local_pool_id = p.first;
      auto &global_image_id = p.second;

      m_image_replayers.emplace_back(librados::IoCtx(), nullptr);
      auto &local_io_ctx = m_image_replayers.back().first;
      auto &image_replayer = m_image_replayers.back().second;

      LocalPoolMeta local_pool_meta;
      r = m_pool_meta_cache->get_local_pool_meta(local_pool_id,
                                                 &local_pool_meta);
      if (r < 0 || local_pool_meta.mirror_uuid.empty()) {
        if (r == 0 || r == -ENOENT) {
          r = -EINVAL;
        }
        derr << "failed to retrieve mirror uuid from local image pool" << dendl;
        break;
      }

      r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     local_pool_id, {}, &local_io_ctx);
      if (r < 0) {
        derr << "failed to open local image pool " << local_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      int64_t remote_pool_id = librados::Rados(m_remote_io_ctx).pool_lookup(
          local_io_ctx.get_pool_name().c_str());

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_pool_id,
                                                  &remote_pool_meta);
      if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
        derr << "failed to retrieve mirror peer uuid from remote image pool"
             << dendl;
        r = -ENOENT;
        break;
      }

      librados::IoCtx remote_io_ctx;
      r = librbd::util::create_ioctx(m_remote_io_ctx, "remote image pool",
                                     remote_pool_id, {}, &remote_io_ctx);
      if (r < 0) {
        derr << "failed to open remote image pool " << remote_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      image_replayer = ImageReplayer<I>::create(
        local_io_ctx, m_local_group_ctx, local_pool_meta.mirror_uuid,
        global_image_id, m_threads, m_instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

      // TODO only a single peer is currently supported
      image_replayer->add_peer({local_pool_meta.mirror_uuid, remote_io_ctx,
                                remote_pool_meta, m_remote_status_updater});
    }
  }

  if (r < 0) {
    for (auto &[_, image_replayer] : m_image_replayers) {
      delete image_replayer;
    }
    m_image_replayers.clear();
    return;
  }

  start_image_replayers();
}

template <typename I>
void Replayer<I>::start_image_replayers() {
  if (is_replay_interrupted()) {
    return;
  }

  dout(10) << m_image_replayers.size() << dendl;

  //set_mirror_group_status_update(
  //  cls::rbd::MIRROR_GROUP_STATUS_STATE_STARTING_REPLAY, "starting replay");

  auto ctx = create_context_callback<
      Replayer, &Replayer<I>::handle_start_image_replayers>(this);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  {
    std::lock_guard locker{m_lock};
    for (auto &[_, image_replayer] : m_image_replayers) {
      image_replayer->start(gather_ctx->new_sub(), false);
    }
  }
  gather_ctx->activate();
}

template <typename I>
void Replayer<I>::handle_start_image_replayers(int r) {
  dout(10) << "r=" << r << dendl;

  if(m_state != STATE_IDLE) {
    finish_start();
  } else {
    set_mirror_group_status_update(
        cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED, "stopped");
    load_local_group_snapshots();
  }
  return;
}

template <typename I>
void Replayer<I>::finish_start() {
  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }
  try_create_group_snapshot(*m_create_group_snap, locker);
  locker.unlock();
  load_local_group_snapshots();
  return;
}

template <typename I>
void Replayer<I>::stop_image_replayer(ImageReplayer<I> *image_replayer,
                                              Context *on_finish) {
  dout(10) << image_replayer << " global_image_id="
           << image_replayer->get_global_image_id() << ", on_finish="
           << on_finish << dendl;

  if (image_replayer->is_stopped()) {
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  m_async_op_tracker.start_op();
  Context *ctx = create_async_context_callback(
    m_threads->work_queue, new LambdaContext(
      [this, image_replayer, on_finish] (int r) {
        stop_image_replayer(image_replayer, on_finish);
        m_async_op_tracker.finish_op();
      }));

  if (image_replayer->is_running()) {
    image_replayer->stop(ctx, false);
  } else {
    int after = 1;
    dout(10) << "scheduling image replayer " << image_replayer << " stop after "
             << after << " sec (task " << ctx << ")" << dendl;
    ctx = new LambdaContext(
      [this, after, ctx] (int r) {
        std::lock_guard timer_locker{m_threads->timer_lock};
        m_threads->timer->add_event_after(after, ctx);
      });
    m_threads->work_queue->queue(ctx, 0);
  }
}

template <typename I>
void Replayer<I>::stop_image_replayers() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<Replayer<I>,
    &Replayer<I>::handle_stop_image_replayers>(this));

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  for (auto &it : m_image_replayers) {
    stop_image_replayer(it.second, gather_ctx->new_sub());
  }
  gather_ctx->activate();
}

template <typename I>
void Replayer<I>::handle_stop_image_replayers(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  {
    std::lock_guard locker{m_lock};

    for (auto &it : m_image_replayers) {
      ceph_assert(it.second->is_stopped());
      it.second->destroy();
    }
    m_image_replayers.clear();
  }

  dout(15) << "waiting for in-flight operations to complete" << dendl;
  m_async_op_tracker.wait_for_ops(new LambdaContext([this](int r) {
        create_replayers();
        }));
}

template <typename I>
void Replayer<I>::set_mirror_group_status_update(
    cls::rbd::MirrorGroupStatusState state, const std::string &desc) {
  dout(20) << "state=" << state << ", description=" << desc << dendl;

  cls::rbd::MirrorGroupSiteStatus local_status;
  local_status.state = state;
  local_status.description = desc;
  local_status.up = true;

  auto remote_status = local_status;

  {
    std::unique_lock locker{m_lock};
    for (auto &[_, ir] : m_image_replayers) {
      cls::rbd::MirrorImageSiteStatus mirror_image;
      if (ir->is_running()) {
        if (ir->is_replaying()) {
          mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING;
        } else {
          mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY;
        }
      } else if (ir->is_stopped()) {
        mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPED;
      } else {
        mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY;
      }
      mirror_image.description = ir->get_state_description();

      local_status.mirror_images[{ir->get_local_pool_id(),
                                  ir->get_global_image_id()}] = mirror_image;
      auto remote_pool_id = ir->get_remote_pool_id();
      if (remote_pool_id >= 0) {
        remote_status.mirror_images[{remote_pool_id,
                                     ir->get_global_image_id()}] = mirror_image;
      }
    }
  }

  m_local_status_updater->set_mirror_group_status(m_global_group_id,
                                                  local_status, true);
  if (m_remote_status_updater != nullptr) {
    m_remote_status_updater->set_mirror_group_status(
        m_global_group_id, remote_status, true);
  }
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;
