exec > >(tee "/var/log/rendezvous_hashing_build.log") 2>&1

cd /tmp

sudo apt-get update -yq
sudo apt-get install -yq git wget

wget -q https://golang.org/dl/go1.17.1.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.17.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
export GOPATH=/usr/local/go
export GOCACHE=/tmp/gocache
mkdir -p "${GOCACHE}"

git clone https://github.com/46bit/distributed-systems.git
cd distributed-systems/rendezvous_hashing
go build -o /tmp/rendezvous_hashing ./cmd/main
sudo mv /tmp/rendezvous_hashing /usr/local/bin/rendezvous_hashing