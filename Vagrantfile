
# NOTE: this Vagrant file will work on linux; for all other machines
# Vagrant is starting a proxy VM and that machine will not be forwarding
# ports properly. try to run it as: FORWARD_DOCKER_PORTS='true' vagrant up 


Vagrant.configure("2") do |config|

  
    #TODO: mount the folder as the user that owns the repo
    config.vm.synced_folder ".", "/vagrant", owner: 1000, group: 130
    
    config.vm.define "app" do |app|
      app.vm.provider "docker" do |d|
        d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
        d.build_dir = "manifests/development/app"
        d.has_ssh = true
        d.name = "app"
        d.create_args = ["--add-host", "dockerhost:" + `ip route | grep docker0 | grep -E -o ' [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+'`.strip]
      end
    end
    
    config.vm.define "db" do |app|
      app.vm.provider "docker" do |d|
        d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
        d.build_dir = "manifests/development/db"
        d.has_ssh = true
        d.name = "db"
        d.ports = ["37017:27017", "6432:5432"]
        #d.volumes = ["data/postgres:/var/lib/postgresql/data", "data/mongodb:/data/db"]
        d.create_args = ["--add-host", "dockerhost:" + `ip route | grep docker0 | grep -E -o ' [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+'`.strip]
      end
    end
    
    config.vm.define "rabbitmq" do |app|
      app.vm.provider "docker" do |d|
        d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
        d.build_dir = "manifests/development/rabbitmq"
        d.has_ssh = true
        d.name = "rabbitmq"
        d.ports = ["6672:5672", "25672:15672"]
        d.create_args = ["--add-host", "dockerhost:" + `ip route | grep docker0 | grep -E -o ' [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+'`.strip]
      end
    end
    
    config.vm.define "imp" do |app|
      app.vm.provider "docker" do |d|
        d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
        d.build_dir = "manifests/development/import-pipeline"
        d.has_ssh = true
        d.name = "imp"
        d.create_args = ["--add-host", "dockerhost:" + `ip route | grep docker0 | grep -E -o ' [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+'`.strip]
      end
    end
  
    config.ssh.username = "root"
    config.ssh.private_key_path = "insecure_key"
    
    config.vm.define "prod" do |prod|
      prod.vm.provider "docker" do |d|
        d.cmd     = ["/sbin/my_init"]
        d.build_dir = "manifests/production/app"
        d.has_ssh = false
        d.name = "ADSOrcid"
        d.remains_running = true
      end
    end
end
