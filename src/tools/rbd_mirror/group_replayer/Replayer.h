// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
#define RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H

#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/AsyncOpTracker.h"
#include "include/rados/librados.hpp"
#include "librbd/mirror/snapshot/Types.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/Types.h"
#include <string>

class Context;

namespace journal { struct CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

struct GroupCtx;
template <typename> struct ImageReplayer;
template <typename> struct InstanceWatcher;
template <typename> struct MirrorStatusUpdater;
struct PoolMetaCache;
template <typename> struct Threads;

namespace group_replayer {

  template <typename ImageCtxT = librbd::ImageCtx>
class Replayer {
public:
  static Replayer* create(
      Threads<ImageCtxT>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT> *local_status_updater,
      MirrorStatusUpdater<ImageCtxT> *remote_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache,
      std::string local_group_id,
      std::string remote_group_id,
      GroupCtx *local_group_ctx) {
    return new Replayer(threads, local_io_ctx, remote_io_ctx, global_group_id,
        local_mirror_uuid, remote_mirror_uuid, instance_watcher,
        local_status_updater, remote_status_updater, cache_manager_handler,
        pool_meta_cache, local_group_id, remote_group_id, local_group_ctx);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT> *local_status_updater,
      MirrorStatusUpdater<ImageCtxT> *remote_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache,
      std::string local_group_id,
      std::string remote_group_id,
      GroupCtx *local_group_ctx);
  ~Replayer();

  void destroy() {
    delete this;
  }
  void init(Context* on_finish);
  void shut_down(Context* on_finish);

  bool is_replaying() const {
    std::unique_lock locker{m_lock};
    return (m_state == STATE_REPLAYING || m_state == STATE_IDLE);
  }
  std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> m_image_replayers;

private:
  enum State {
    STATE_INIT,
    STATE_REPLAYING,
    STATE_IDLE,
    STATE_COMPLETE
  };

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  std::string m_global_group_id;
  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  MirrorStatusUpdater<ImageCtxT> *m_local_status_updater;
  MirrorStatusUpdater<ImageCtxT> *m_remote_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;
  std::string m_local_group_id;
  std::string m_remote_group_id;
  GroupCtx *m_local_group_ctx;
  std::map<std::pair<int64_t, std::string>, ImageReplayer<ImageCtxT> *> m_image_replayer_index;

  mutable ceph::mutex m_lock;

  State m_state = STATE_INIT;
  std::string m_remote_mirror_peer_uuid;

  std::vector<cls::rbd::GroupSnapshot> m_local_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> m_remote_group_snaps;
  cls::rbd::GroupSnapshot *m_create_group_snap;
  cls::rbd::GroupSnapshot *m_last_local_group_snap = nullptr;

  bool m_stop_requested = false;
  AsyncOpTracker m_async_op_tracker;

  // map of <group_snap_id, pair<GroupSnapshot, on_finish>>
  std::map<std::string, std::pair<cls::rbd::GroupSnapshot, Context *>> m_create_snap_requests;

  // map of <group_snap_id, vec<pair<cls::rbd::ImageSnapshotSpec, bool>>>
  std::map<std::string, std::vector<std::pair<cls::rbd::ImageSnapshotSpec, bool>>> m_pending_group_snaps;

  typedef std::pair<int64_t /*pool_id*/, std::string /*global_image_id*/> GlobalImageId;
  std::map<GlobalImageId, std::string /*image_id */> m_snap_members;
  //int create_replayers();

  int local_group_image_list_by_id(
      std::vector<cls::rbd::GroupImageStatus> *image_ids);

  bool is_replay_interrupted();
  bool is_replay_interrupted(std::unique_lock<ceph::mutex>* lock);

  void notify_group_listener_stop();
  bool is_resync_requested();
  bool is_rename_requested();

  void load_local_group_snapshots();
  void handle_load_local_group_snapshots(int r);

  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);

  void validate_image_snaps_sync_complete(const std::string &remote_group_snap_id);
  bool is_membership_changed(cls::rbd::GroupSnapshot next_remote_snap);
  void scan_for_unsynced_group_snapshots();

  //void create_replayers(cls::rbd::GroupSnapshot *group_snap);
  void create_replayers();

  void start_image_replayers();
  void handle_start_image_replayers(int r);
  void finish_start();

  void stop_image_replayer(ImageReplayer<ImageCtxT> *image_replayer,
                           Context *on_finish);

  void stop_image_replayers();
  void handle_stop_image_replayers(int r);

  void try_create_group_snapshot(cls::rbd::GroupSnapshot snap,
                                 std::unique_lock<ceph::mutex> &locker);

  void create_mirror_snapshot(
    const std::string &remote_group_snap_id,
    const cls::rbd::MirrorSnapshotState &snap_state, Context *on_finish);
  void handle_create_mirror_snapshot(
    int r, const std::string &remote_group_snap_id, Context *on_finish);

  std::string prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id, const std::string &snap_id);

  void mirror_snapshot_complete(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec *spec,
    Context *on_finish);
  void handle_mirror_snapshot_complete(
    int r, const std::string &remote_group_snap_id, Context *on_finish);

  void unlink_group_snapshots(const std::string &remote_group_snap_id,
                              Context *on_finish);

  void remove_image_snapshot(std::string image_id,
                             uint64_t snap_id, int64_t pool_id);
  void create_regular_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    Context *on_finish);
  void handle_create_regular_snapshot(int r, Context *on_finish);
  void regular_snapshot_complete(
    const std::string &remote_group_snap_id,
    Context *on_finish);
  void handle_regular_snapshot_complete(int r, Context *on_finish);
  void set_mirror_group_status_update(
    cls::rbd::MirrorGroupStatusState state, const std::string &desc);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
