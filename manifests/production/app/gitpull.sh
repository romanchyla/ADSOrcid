#/bin/bash -ev

git config --global user.name "anon"
git config --global user.email "anon@anon.com"
cd /app


git fetch
git fetch --tags

latest_tag=`git describe --tags $(git rev-list --tags --max-count=1)`

if [ -f latest-production ]; then
    latest=`cat latest-production`
    if [ "$latest" == "$latest_tag" ]; then
      exit 0
    fi
fi

echo `date` "Deploying $latest_tag" >> /var/log/automated-pulls

# checkout latest release tag
git checkout --force $latest_tag

#Provision libraries/database
pip install -r requirements.txt
alembic upgrade head

echo $latest_tag > latest-production

# restart the service
sv restart app
