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

