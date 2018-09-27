Scale Out Storage - client library
==================================

SURFsara provides an iRODS service that interfaces the tape archive. The user can use iput and iget to put to and
retrieve files from the archive. While the iput operation is synchronous (from the user perspective), the iget
schedules an retrieval process on archive server.

This software package provides a set of command line applications and a local daemon on the client 
side that handles asynchronous file download and upload using iRODS as the backend.

Architecture
------------
The client application consists of 6 components:
- a daemopn that handles file transfer in the background.
- 5 command line tools for mannaging the daemon and controlling file transfer.

![architecture](https://raw.githubusercontent.com/sara-nl/iRODS_DMF_client/master/doc/arch.png)

Installation
------------

The client can be installed with pip:
    
    pip install git+https://github.com/stefan-wolfsheimer/py-socket-server.git#egg=py-socket-server
    pip install git+https://github.com/sara-nl/iRODS_DMF_client.git#egg=irods-dmf-client

or using a virtual environment (recommented)

    pipenv install git+https://github.com/stefan-wolfsheimer/py-socket-server.git#egg=py-socket-server
    pipenv install git+https://github.com/sara-nl/iRODS_DMF_client.git#egg=irods-dmf-client

Usage
-----
When you are using a virtual environment, invoke a shell using pipenv

    pipenv shell

### dm_iconfig

The script *dm_iconfig* is used to configure the connection to iRODS.
There are two options:
1. using an *irods_environment.json* and *password* file configured through iinit
   (requires icommands)
2. If icommands are not installed the scripts asks you to configure iRODS manually.
   The password is required upon the first connection to the iRODS server.

The resulting configuration is stored in the following file:

    ~/.DmIRodsServer/config.json

### dm_ilist

*dm_ilist* lists all iRODS data objects on the DMF resource.

Example:

    > dm_ilist
    DMF FILE                                    TIME                STATUS
    DUL /surf/home/rods/test50.mb
    OFL /surf/home/rods/subdir/100Mfile

The first column is the DMF state of the file, while TIME and STATUS refer to the
download/upload time and state.

### dm_iget

*dm_iget* can be used to download a file from the archive. It is scheduled in
the background. (type *dm_iget --help* for more details)

Example:

    > dm_iget /surf/home/rods/test50.mb
    STATUS              FILE
    scheduled           /surf/home/rods/test50.mb 

Immediatly after requesting a file the state has changed:

    > dm_ilist
    DMF FILE                                    TIME                STATUS
    DUL /surf/home/rods/test50.mb               2018-09-12 15:06:47 GETTING
    OFL /surf/home/rods/subdir/100Mfile

After a few minutes the state will change to DONE

    > dm_ilist
    DMF FILE                                    TIME                STATUS
    DUL /surf/home/rods/test50.mb               2018-09-12 15:06:47 DONE
    OFL /surf/home/rods/subdir/100Mfile


### dm_iput

    > dm_iput test50.mb
    STATUS              FILE
    scheduled           .../test50mb <> /{zone}/home/{user}/test50mb

Immediatly after putting a file the state has changed:

    > dm_ilist
    DMF FILE                               TIME                STATUS     MOD 
    /surf/home/rods/test50mb               2018-09-12 12:43:00 PUTTING    PUT 



### dm_idaemon

The daemon is automatically started when using one of the command line applications
that communicate with iRODS. However it is possible to controll the daemon manually as well.
The following commands are available (type *dm_idaemon --help* for more details):


**check if the daemon is running**

    dm_idaemon status

**start the daemon**

    dm_idaemon start

**restart the daemon**

    dm_idaemon restart

**stop the daemon**

    dm_idaemon stop

**run the daemon in the console**

    dm_idaemon

**Note**: the state of the daemon (i.e. files to be transfered) is persistent.
The information is stored in ~/.DmIRodsServer/Tickets
