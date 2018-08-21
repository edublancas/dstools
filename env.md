
The env should be flexible enough to let users
organize the project as they desire, and only
provide minimal, but useful functionality.

It should also provide other enhancements for people
who want to follow the template (convention over configuration),
some of these ideas are:

* a project environment is defined by a single env.yaml file
* if no name is provided, is assigned "default"
* then i must check env.default.yaml does not exist
* an optional name can be provided env.name.yaml
* no parameters in the file are mandatory


Defaults:

* project_home is where the env.yaml file is located
* data in project_home/{env_name}/{data}/
* log in project_home/{env_name}/{log}/

