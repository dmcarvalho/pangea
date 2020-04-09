#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def setup_remote_debugging(force_enabled=False):
    """ Programaticaly enables remote debugging if SC_BOOT_MODE==debug-ptvsd

    """
    boot_mode = os.environ["SC_BOOT_MODE"]  if 'SC_BOOT_MODE' in  os.environ else "Visual Studio"

    if boot_mode == "debug-ptvsd" or force_enabled:
        try:
            print("Enabling attach ptvsd ...")
            #
            # SEE https://github.com/microsoft/ptvsd#enabling-debugging
            #
            import ptvsd
            ptvsd.enable_attach(address=('0.0.0.0', 3005))

        except ImportError:
            print("Unable to use remote debugging. ptvsd is not installed")

        else:
            print("Remote debugging enabled")
    else:
        print("Booting without remote debugging since SC_BOOT_MODE=%s" % boot_mode) 



def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pangea.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)

if __name__ == '__main__':
    if os.environ.get('RUN_MAIN') == 'true':
        setup_remote_debugging()
    main()

