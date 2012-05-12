# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2010 Openstack, LLC.
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Simple Scheduler
"""

import glob
import os

from nova import db
from nova import flags
from nova import utils
from nova.scheduler import driver
from nova.scheduler import chance
from os import statvfs
from nova import log as logging

FLAGS = flags.FLAGS
flags.DEFINE_integer("max_cores", 16,
    "maximum number of instance cores to allow per host")
flags.DEFINE_integer("max_gigabytes", 10000,
    "maximum number of volume gigabytes to allow per host")
flags.DEFINE_integer("max_networks", 1000,
    "maximum number of networks to allow per host")
flags.DEFINE_string("shared_storage_folder", '/var/lib/nova/volumes', "Shared file storage")
flags.DEFINE_string("disk_images_folder", "/var/lib/glance/images/", "Images storage folder")
flags.DEFINE_bool("share_images_and_instances", True, "Share the instances and images folder")

LOG = logging.getLogger('nova.scheduler.vcpuandspace')

class VCPUAndSpaceScheduler(chance.ChanceScheduler):
    """Implements a Shared folder Scheduler that checks available space in it"""

    def check_images_size(self, images_path='/var/lib/glance/images/'):
        total_images_size = 0
        for name in glob.glob('%s[0-9]*' % images_path):
            size = os.path.getsize(name)
            total_images_size += size
        return total_images_size / 1073741824

    def _check_disk_space(self, context):
        results = db.instance_get_all(context)
        total_vm_disk_size = 0
        for result in results:
            local_gb = result.local_gb
            total_vm_disk_size = total_vm_disk_size + local_gb
        LOG.debug('Total VM disk space = %s' % total_vm_disk_size)
        results = db.service_get_all_volume_sorted(context)
        total_volumes_disk_size = 0
        for result in results:
            (service, volume_gigabytes) = result
            total_volumes_disk_size = total_volumes_disk_size + volume_gigabytes
        LOG.debug('Total Volumes disk space = %s' % total_volumes_disk_size)
        return total_vm_disk_size + total_volumes_disk_size

    def _schedule_instance(self, context, instance_id, *_args, **_kwargs):
        """Picks a host that is up and has the fewest running instances."""
        instance_ref = db.instance_get(context, instance_id)
        if (instance_ref['availability_zone']
            and ':' in instance_ref['availability_zone']
            and context.is_admin):
            zone, _x, host = instance_ref['availability_zone'].partition(':')
            service = db.service_get_by_args(context.elevated(), host,
                'nova-compute')
            if not self.service_is_up(service):
                raise driver.WillNotSchedule(_("Host %s is not alive") % host)

            # TODO(vish): this probably belongs in the manager, if we
            #             can generalize this somehow
            now = utils.utcnow()
            db.instance_update(context, instance_id, {'host': host,
                                                      'scheduled_at': now})
            return host
        fs = statvfs(FLAGS.shared_storage_folder)
        total_disk_size = fs.f_blocks * fs.f_frsize / 1073741824
        if FLAGS.share_images_and_instances:
            total_disk_size -= self.check_images_size(FLAGS.disk_images_folder)
        LOG.debug('Total disk space available for instances and volumes= %s' % total_disk_size)
        disk_space = self._check_disk_space(context)
        if disk_space > total_disk_size:
            raise driver.NoValidHost(_("Not enough space for VMs:%sGB" % disk_space))
        results = db.service_get_all_compute_sorted(context)
        for result in results:
            (service, instance_cores) = result
            if instance_cores + instance_ref['vcpus'] > FLAGS.max_cores:
                raise driver.NoValidHost(_("All hosts have too many cores"))
            if self.service_is_up(service):
                # NOTE(vish): this probably belongs in the manager, if we
                #             can generalize this somehow
                now = utils.utcnow()
                db.instance_update(context,
                    instance_id,
                        {'host': service['host'],
                         'scheduled_at': now})
                return service['host']
        raise driver.NoValidHost(_("Scheduler was unable to locate a host"
                                   " for this request. Is the appropriate"
                                   " service running?"))

    def schedule_run_instance(self, context, instance_id, *_args, **_kwargs):
        return self._schedule_instance(context, instance_id, *_args, **_kwargs)

    def schedule_start_instance(self, context, instance_id, *_args, **_kwargs):
        return self._schedule_instance(context, instance_id, *_args, **_kwargs)

    def schedule_create_volume(self, context, volume_id, *_args, **_kwargs):
        """Picks a host that is up and has the fewest volumes."""
        volume_ref = db.volume_get(context, volume_id)
        if (volume_ref['availability_zone']
            and ':' in volume_ref['availability_zone']
            and context.is_admin):
            zone, _x, host = volume_ref['availability_zone'].partition(':')
            service = db.service_get_by_args(context.elevated(), host,
                'nova-volume')
            if not self.service_is_up(service):
                raise driver.WillNotSchedule(_("Host %s not available") % host)

            # TODO(vish): this probably belongs in the manager, if we
            #             can generalize this somehow
            now = utils.utcnow()
            db.volume_update(context, volume_id, {'host': host,
                                                  'scheduled_at': now})
            return host
        fs = statvfs(FLAGS.shared_storage_folder)
        total_disk_size = fs.f_blocks * fs.f_frsize / 1073741824
        if FLAGS.share_images_and_instances:
            total_disk_size -= self.check_images_size(FLAGS.disk_images_folder)
        LOG.debug('Total disk space available for instances and volumes= %s' % total_disk_size)
        disk_space = self._check_disk_space(context) + volume_ref['size']
        if disk_space > total_disk_size:
            raise driver.NoValidHost(_("Not enough space for VMs:%sGB" % disk_space))
        results = db.service_get_all_volume_sorted(context)
        for result in results:
            (service, volume_gigabytes) = result
            if self.service_is_up(service):
                # NOTE(vish): this probably belongs in the manager, if we
                #             can generalize this somehow
                now = utils.utcnow()
                db.volume_update(context,
                    volume_id,
                        {'host': service['host'],
                         'scheduled_at': now})
                return service['host']
        raise driver.NoValidHost(_("Scheduler was unable to locate a host"
                                   " for this request. Is the appropriate"
                                   " service running?"))

    def schedule_set_network_host(self, context, *_args, **_kwargs):
        """Picks a host that is up and has the fewest networks."""

        results = db.service_get_all_network_sorted(context)
        for result in results:
            (service, instance_count) = result
            if instance_count >= FLAGS.max_networks:
                raise driver.NoValidHost(_("All hosts have too many networks"))
            if self.service_is_up(service):
                return service['host']
        raise driver.NoValidHost(_("Scheduler was unable to locate a host"
                                   " for this request. Is the appropriate"
                                   " service running?"))
