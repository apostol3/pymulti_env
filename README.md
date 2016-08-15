# Multiplexer utility for nlab

require pynlab

````
usage: multi_env.py [-h] [-O name] [-I name] [-e] N exec

positional arguments:
  N                     count of environments to start
  exec                  command to execute environments

optional arguments:
  -h, --help            show this help message and exit
  -O name, --nlab-pipe name
                        nlab pipe name (default: nlab)
  -I name, --envs-pipe name
                        enviroments pipe name (default: nlab_mlt)
  -e, --existing        connect to existing environments and do not spawn them
````

###Rough usage diagram:
![diagram](.\diagram.png)
