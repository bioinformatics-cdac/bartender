docker build --progress=plain  -t icr.icecloud.in/k8s/bartender-bar -f deploy/Dockerfile.bartender .

docker build --progress=plain  -t icr.icecloud.in/k8s/bartender-k8s -f deploy/Dockerfile .
