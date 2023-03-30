import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '../models/rgw-multisite';
import { ModalService } from '~/app/shared/services/modal.service';
import { RgwSystemUserComponent } from '../rgw-system-user/rgw-system-user.component';

@Component({
  selector: 'cd-rgw-multisite-zone-form',
  templateUrl: './rgw-multisite-zone-form.component.html',
  styleUrls: ['./rgw-multisite-zone-form.component.scss']
})
export class RgwMultisiteZoneFormComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  action: string;
  info: any;
  multisiteZoneForm: CdFormGroup;
  editing = false;
  resource: string;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  zone: RgwZone;
  defaultsInfo: string[] = [];
  multisiteInfo: object[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];
  users: any;
  placementTargets: any;
  zoneInfo: RgwZone;
  poolList: object[] = [];
  storageClassList: object[] = [];
  disableDefault: boolean = false;
  disableMaster: boolean = false;
  isMetadataSync: boolean = false;
  isMasterZone: boolean;
  isDefaultZone: boolean;
  syncStatusTimedOut: boolean = false;
  bsModalRef: NgbModalRef;
  createSystemUser: boolean = false;
  master_zone_of_master_zonegroup: RgwZone;
  masterZoneUser: any;
  access_key: any;
  master_zonegroup_of_realm: RgwZonegroup;
  compressionTypes = ['lz4', 'zlib', 'snappy'];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwMultisiteService: RgwMultisiteService,
    public rgwZoneService: RgwZoneService,
    public rgwZoneGroupService: RgwZonegroupService,
    public notificationService: NotificationService,
    public rgwUserService: RgwUserService,
    public modalService: ModalService
  ) {
    this.action = this.editing
      ? this.actionLabels.EDIT + this.resource
      : this.actionLabels.CREATE + this.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteZoneForm = new CdFormGroup({
      zoneName: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zoneName: string) => {
            return (
              this.action === 'create' && this.zoneNames && this.zoneNames.indexOf(zoneName) !== -1
            );
          })
        ]
      }),
      default_zone: new FormControl(false),
      master_zone: new FormControl(false),
      selectedZonegroup: new FormControl(null),
      zone_endpoints: new FormControl([], {
        validators: [
          CdValidators.custom('endpoint', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            } else {
              if (value.includes(',')) {
                value.split(',').forEach((url: string) => {
                  return (
                    !this.endpoints.test(url) && !this.ipv4Rgx.test(url) && !this.ipv6Rgx.test(url)
                  );
                });
              } else {
                return (
                  !this.endpoints.test(value) &&
                  !this.ipv4Rgx.test(value) &&
                  !this.ipv6Rgx.test(value)
                );
              }
              return false;
            }
          }),
          Validators.required
        ]
      }),
      users: new FormControl(null),
      placementTarget: new FormControl(null),
      placementDataPool: new FormControl(''),
      placementIndexPool: new FormControl(null),
      placementDataExtraPool: new FormControl(null),
      storageClass: new FormControl(null),
      storageDataPool: new FormControl(null),
      storageCompression: new FormControl(null)
    });
  }

  onZoneGroupChange(zonegroupName: string) {
    let zg = new RgwZonegroup();
    zg.name = zonegroupName;
    this.rgwZoneGroupService.get(zg).subscribe((zonegroup: RgwZonegroup) => {
      if (_.isEmpty(zonegroup.master_zone)) {
        this.multisiteZoneForm.get('master_zone').setValue(true);
        this.multisiteZoneForm.get('master_zone').disable();
        this.disableMaster = false;
      } else if (!_.isEmpty(zonegroup.master_zone) && this.action === 'create') {
        this.multisiteZoneForm.get('master_zone').setValue(false);
        this.multisiteZoneForm.get('master_zone').disable();
        this.disableMaster = true;
      }
      const zonegroupInfo = this.zonegroupList.filter((zgroup: any) => zgroup.name === zg.name)[0];
      if (zonegroupInfo) {
        const realm_id = zonegroupInfo.realm_id;
        this.master_zonegroup_of_realm = this.zonegroupList.filter(
          (zg: any) => zg.realm_id === realm_id && zg.is_master === true
        )[0];
      }
      if (this.master_zonegroup_of_realm) {
        this.master_zone_of_master_zonegroup = this.zoneList.filter(
          (zone: any) => zone.id === this.master_zonegroup_of_realm.master_zone
        )[0];
      }
      if (this.master_zone_of_master_zonegroup) {
        this.getUserInfo(this.master_zone_of_master_zonegroup);
      }
      if (zonegroupInfo.is_master && this.multisiteZoneForm.getValue('master_zone') === true) {
        this.createSystemUser = true;
      }
    });
    if (
      this.multisiteZoneForm.getValue('selectedZonegroup') !==
      this.defaultsInfo['defaultZonegroupName']
    ) {
      this.disableDefault = true;
      this.multisiteZoneForm.get('default_zone').disable();
    }
  }

  ngOnInit(): void {
    this.zonegroupList =
      this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
        ? this.multisiteInfo[1]['zonegroups']
        : [];
    this.zoneList =
      this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
        ? this.multisiteInfo[2]['zones']
        : [];
    this.zoneNames = this.zoneList.map((zone) => {
      return zone['name'];
    });
    if (this.action === 'create') {
      if (this.defaultsInfo['defaultZonegroupName'] !== undefined) {
        this.multisiteZoneForm
          .get('selectedZonegroup')
          .setValue(this.defaultsInfo['defaultZonegroupName']);
        this.onZoneGroupChange(this.defaultsInfo['defaultZonegroupName']);
      }
    }
    if (this.action === 'edit') {
      this.placementTargets = this.info.parent ? this.info.parent.data.placement_targets : [];
      this.rgwZoneService.getPoolNames().subscribe((pools: object[]) => {
        this.poolList = pools;
      });
      this.multisiteZoneForm.get('zoneName').setValue(this.info.data.name);
      this.multisiteZoneForm.get('selectedZonegroup').setValue(this.info.data.parent);
      this.multisiteZoneForm.get('default_zone').setValue(this.info.data.is_default);
      this.multisiteZoneForm.get('master_zone').setValue(this.info.data.is_master);
      this.multisiteZoneForm.get('zone_endpoints').setValue(this.info.data.endpoints);
      this.multisiteZoneForm
        .get('placementTarget')
        .setValue(this.info.parent.data.default_placement);
      this.getZonePlacementData(this.multisiteZoneForm.getValue('placementTarget'));
      if (this.info.data.is_default) {
        this.isDefaultZone = true;
        this.multisiteZoneForm.get('default_zone').disable();
      }
      if (this.info.data.is_master) {
        this.isMasterZone = true;
        this.multisiteZoneForm.get('master_zone').disable();
      }
      const zone = new RgwZone();
      zone.name = this.info.data.name;
      this.onZoneGroupChange(this.info.data.parent);
      setTimeout(() => {
        this.getUserInfo(zone);
      }, 1500);
    }
    if (
      this.multisiteZoneForm.getValue('selectedZonegroup') !==
      this.defaultsInfo['defaultZonegroupName']
    ) {
      this.disableDefault = true;
      this.multisiteZoneForm.get('default_zone').disable();
    }
  }

  getUserInfo(zone: RgwZone) {
    this.rgwZoneService
      .getUserList(this.master_zone_of_master_zonegroup.name)
      .subscribe((users: any) => {
        this.users = users.filter((user: any) => user.keys.length !== 0);
        this.rgwZoneService.get(zone).subscribe((zone: RgwZone) => {
          const access_key = zone.system_key['access_key'];
          const user = this.users.filter((user: any) => user.keys[0].access_key === access_key);
          if (user.length > 0) {
            this.multisiteZoneForm.get('users').setValue(user[0].user_id);
          }
          return user[0].user_id;
        });
      });
  }

  getZonePlacementData(placementTarget: string) {
    this.zone = new RgwZone();
    this.zone.name = this.info.data.name;
    if (this.placementTargets) {
      this.placementTargets.forEach((placement: any) => {
        if (placement.name === placementTarget) {
          let storageClasses = placement.storage_classes;
          this.storageClassList = Object.entries(storageClasses).map(([key, value]) => ({
            key,
            value
          }));
        }
      });
    }
    this.rgwZoneService.get(this.zone).subscribe((zoneInfo: RgwZone) => {
      this.zoneInfo = zoneInfo;
      if (this.zoneInfo && this.zoneInfo['placement_pools']) {
        this.zoneInfo['placement_pools'].forEach((plc_pool) => {
          if (plc_pool.key === placementTarget) {
            let storageClasses = plc_pool.val.storage_classes;
            let placementDataPool = storageClasses['STANDARD']
              ? storageClasses['STANDARD']['data_pool']
              : '';
            let placementIndexPool = plc_pool.val.index_pool;
            let placementDataExtraPool = plc_pool.val.data_extra_pool;
            this.poolList.push({ poolname: placementDataPool });
            this.poolList.push({ poolname: placementIndexPool });
            this.poolList.push({ poolname: placementDataExtraPool });
            this.multisiteZoneForm.get('storageClass').setValue(this.storageClassList[0]['value']);
            this.multisiteZoneForm.get('storageDataPool').setValue(placementDataPool);
            this.multisiteZoneForm.get('storageCompression').setValue(this.compressionTypes[0]);
            this.multisiteZoneForm.get('placementDataPool').setValue(placementDataPool);
            this.multisiteZoneForm.get('placementIndexPool').setValue(placementIndexPool);
            this.multisiteZoneForm.get('placementDataExtraPool').setValue(placementDataExtraPool);
          }
        });
      }
    });
  }

  getStorageClassData(storageClass: string) {
    let storageClassSelected = this.storageClassList.find((x) => x['value'] == storageClass)[
      'value'
    ];
    this.poolList.push({ poolname: storageClassSelected.data_pool });
    this.multisiteZoneForm.get('storageDataPool').setValue(storageClassSelected.data_pool);
    this.multisiteZoneForm
      .get('storageCompression')
      .setValue(storageClassSelected.compression_type);
  }

  submit() {
    const values = this.multisiteZoneForm.getRawValue();
    if (this.action === 'create') {
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = values['selectedZonegroup'];
      this.zone = new RgwZone();
      this.zone.name = values['zoneName'];
      this.zone.endpoints = this.checkUrlArray(values['zone_endpoints']);
      if (this.createSystemUser) {
        values['users'] = values['zoneName'] + '_User';
      }
      this.rgwZoneService
        .create(
          this.zone,
          this.zonegroup,
          values['default_zone'],
          values['master_zone'],
          this.zone.endpoints,
          values['users'],
          this.createSystemUser,
          this.master_zone_of_master_zonegroup
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zone: '${values['zoneName']}' created successfully`
            );
            this.activeModal.close();
          },
          () => {
            this.multisiteZoneForm.setErrors({ cdSubmitButton: true });
          }
        );
    } else if (this.action === 'edit') {
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = values['selectedZonegroup'];
      this.zone = new RgwZone();
      this.zone.name = this.info.data.name;
      this.zone.endpoints =
        values['zone_endpoints'] === this.info.data.endpoints
          ? values['zonegroup_endpoints']
          : this.checkUrlArray(values['zone_endpoints']);
      this.rgwZoneService
        .update(
          this.zone,
          this.zonegroup,
          values['zoneName'],
          values['default_zone'],
          values['master_zone'],
          this.zone.endpoints,
          values['users'],
          values['placementTarget'],
          values['placementDataPool'],
          values['placementIndexPool'],
          values['placementDataExtraPool'],
          values['storageClass'],
          values['storageDataPool'],
          values['storageCompression'],
          this.master_zone_of_master_zonegroup
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zone: '${values['zoneName']}' updated successfully`
            );
            this.activeModal.close();
          },
          () => {
            this.multisiteZoneForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }

  checkUrlArray(endpoints: string) {
    let endpointsArray = [];
    if (endpoints.includes(',')) {
      endpointsArray = endpoints.split(',');
    } else {
      endpointsArray.push(endpoints);
    }
    return endpointsArray;
  }

  CreateSystemUser() {
    const initialState = {
      zoneName: this.master_zone_of_master_zonegroup.name
    };
    this.bsModalRef = this.modalService.show(RgwSystemUserComponent, initialState);
    this.bsModalRef.componentInstance.submitAction.subscribe(() => {
      this.getUserInfo(this.master_zone_of_master_zonegroup);
    });
  }
}
