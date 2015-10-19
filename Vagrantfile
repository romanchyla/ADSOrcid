Vagrant.configure("2") do |config|
  #TODO: mount the folder as the user that owns the repo
  config.vm.synced_folder ".", "/vagrant", owner: 1000, group: 130
  
  config.vm.define "app" do |app|
    app.vm.provider "docker" do |d|
      d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
      d.build_dir = "manifests/development/app"
      d.has_ssh = true
      d.name = "app"
    end
  end
  
  config.vm.define "db" do |app|
    app.vm.provider "docker" do |d|
      d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
      d.build_dir = "manifests/development/db"
      d.has_ssh = true
      d.name = "db"
    end
  end
  
  config.vm.define "rabbitmq" do |app|
    app.vm.provider "docker" do |d|
      d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
      d.build_dir = "manifests/development/rabbitmq"
      d.has_ssh = true
      d.name = "rabbitmq"
    end
  end

  config.ssh.username = "root"
  config.ssh.private_key_path = "insecure_key"
  
end