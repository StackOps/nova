# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 STACKOPS TECHNOLOGIES S.L.
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
Drivers for nas volumes.

The unique thing about a SAN is that we don't expect that we can run the volume
controller on the SAN hardware.  We expect to access it over SSH or some API.
"""



from nova import exception
from nova import flags
from nova import log as logging
from nova.volume.driver import VolumeDriver

LOG = logging.getLogger("nova.volume.driver")
FLAGS = flags.FLAGS
flags.DEFINE_string('volumes_path', '/var/lib/nova/volumes', 'shared directory for the volumes virtual disks')
flags.DEFINE_string('volumes_path_testfile', '%s/testfile' % FLAGS.volumes_path, 'Test file to check if qemu-img works')

class QEMUDriver(VolumeDriver):
    """Executes commands relating to QEMU virtual disks volumes.

    """

    def __init__(self, *args, **kwargs):
        LOG.info(_("Initializing QEMU virtual block disk driver."))
        super(QEMUDriver, self).__init__(*args, **kwargs)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        try:
            out, err = self._execute('qemu-img', 'create', '-f', 'qcow2', '-o', 'cluster_size=2M',
                FLAGS.volumes_path_testfile, '1M')
            if err:
                self._execute('rm', FLAGS.volumes_path_testfile)
                raise exception.Error(_("Cannot create the test file in " % FLAGS.volumes_path_testfile))
            out, err = self._execute('qemu-img', 'info', FLAGS.volumes_path_testfile)
            if err:
                self._execute('rm', FLAGS.volumes_path_testfile)
                raise exception.Error(_("Cannot retrieve info about the test file in " % FLAGS.volumes_path_testfile))
            self._execute('rm', FLAGS.volumes_path_testfile)
        except exception.ProcessExecutionError:
            raise exception.Error(_("qemu-img virtual disk creation failed. QEMU Driver cannot be initialized."))


    def create_volume(self, volume):
        """Creates a virtual disk as a volume."""
        self._try_execute('qemu-img', 'create', '-f', 'qcow2', '-o', 'cluster_size=2M',
            "%s/%s" % (FLAGS.volumes_path, volume['name']), self._sizestr(volume['size']))

    def delete_volume(self, volume):
        """Deletes a virtual disk as a  volume. We assume disks without backing files"""
        self._execute('rm', '%s/%s' % (FLAGS.volumes_path, volume['name']))

    def create_snapshot(self, snapshot):
        """Creates a snapshot from a virtual disk"""
        self._try_execute('qemu-img', 'snapshot', '-c', snapshot['name'],
            "%s/%s" % (FLAGS.volumes_path, snapshot['volume_name']))

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot from a virtual disk"""
        self._try_execute('qemu-img', 'snapshot', '-d', snapshot['name'],
            "%s/%s" % (FLAGS.volumes_path, snapshot['volume_name']))

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        raise NotImplementedError()

    def local_path(self, volume):
        return '%s/%s' % (FLAGS.volumes_path, volume['name'])

    def ensure_export(self, context, volume):
        """Safely and synchronously recreates an export for a logical volume"""
        pass

    def create_export(self, context, volume):
        """Exports the volume"""
        pass

    def remove_export(self, context, volume):
        """Removes an export for a logical volume"""
        pass

    def discover_volume(self, context, volume):
        """Discover volume """
        return self.local_path(volume)

    def undiscover_volume(self, volume):
        """Undiscover volume on a remote host"""
        pass

    def get_volume_stats(self, refresh=False):
        """Return the current state of the volume service. If 'refresh' is
           True, run the update first."""
        return None
