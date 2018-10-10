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
    
    pip install git+https://github.com/sara-nl/iRODS_DMF_client.git#egg=irods-dmf-client

or using a virtual environment (recommented)

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

### auto completion 

The configuration script *dm_iconfig* will also create a shell script to
support autocompletion for two dm_i - commands (*dm_iget* and *dm_iinit*).
To enable autocomplete in the current shell, just source the autocompletion file

    source ~/.DmIRodsServer/completion.sh

To enable this functionally for all future shell sessions, it is possible to configure
auto completion in *.bashrc*

    cat ~/.DmIRodsServer/completion.sh >> ~/.bashrc


### dm_ilist

*dm_ilist* lists all iRODS data objects on the DMF resource.

Example:

    > dm_ilist
    DMF TIME                STATUS         MOD FILE                              LOCAL_FILE
    DUL 2018-10-10 15:52:38 DONE      100% GET /surf/home/rods/1M_0003.dat       1M_0003.dat
    DUL 2018-10-10 16:31:52 DONE      100% PUT /surf/home/rods/1M_0001.dat       1M_0001.dat
    DUL                                        /surf/home/rods/1G_0001.dat
    DUL                                        /surf/home/rods/

The first column is the DMF state of the file, while TIME and STATUS refer to the
download/upload time and state.

### dm_iget

*dm_iget* can be used to download a file from the archive. It is scheduled in
the background. (type *dm_iget --help* for more details)

Example:

    > dm_iget /surf/home/rods/test50.mb
    STATUS              FILE
    scheduled           /surf/home/rods/test50.mb 

Immediatly after requesting a file the state has changed (can be checked with *dm_ilist*).
After a few minutes the state will change to DONE
It is also possible to monitor the state of the files.

    > dm_ilist -w

In this case, the screen refreshes periodically similar to the unix *watch* command.


### dm_iput

    > dm_iput test50.mb
    STATUS              FILE
    scheduled           .../test50mb <> /{zone}/home/{user}/test50mb

Immediatly after putting a file the state has changed:

    > dm_ilist
    DMF TIME                STATUS         MOD FILE                       LOCAL_FILE
    DUL 2018-10-10 16:31:52 PUTTING    30% PUT /surf/home/rods/test50.mb  test50.mb


### dm_iinfo

The command *dm_iinfo* can be used to retrieve details on a certain object.
The result is divided in 4 blocks:
 * Transfer related information (if the object has been transferred with this tool)
 * Data on the local file
 * iRODS related information
 * DMF related information
 
For example:

    > dm_iinfo /surf/home/rods/1M_0003.dat
    --------------------------
    Transfer 
    --------------------------
    retries                : 3
    status                 : DONE
    errmsg                 :
    time_created           : 2018-10-10 15:52:38
    transferred            : 1048576
    mode                   : GET
    --------------------------
    Local File
    --------------------------
    local_file             : ~/1M_0003.dat
    checksum               : 4rOHOreFDi9BgY9dHBg0dS92kgV0ChXzj0U+dEBfXe0=
    ...
    --------------------------
    Remote Object
    --------------------------
    remote_file            : /surf/home/rods/1M_0003.dat
    remote_size            : 1048576
    remote_create_time     : 2018-10-02 13:48:07
    ...
    --------------------------
    DMF Data
    --------------------------
    DMF_state              : DUL
    DMF_emask              : 160000
    ...



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
Apache License
==============

Copyright 2018 SURFsara BV
    
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    
http://www.apache.org/licenses/LICENSE-2.0
    
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
