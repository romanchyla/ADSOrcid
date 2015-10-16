Vagrant.configure("2") do |config|
  #TODO: mount the folder as the user that owns the repo
  config.vm.synced_folder ".", "/vagrant", owner: 1000, group: 130
  config.vm.provider "docker" do |d|
    d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
    #d.image   = "phusion/baseimage:0.9.17"
    d.build_dir = "."
    d.has_ssh = true
  end

  config.ssh.username = "root"
  config.ssh.private_key_path = "insecure_key"
  
end