# 1. Start Minikube
minikube start --cpus=4 --memory=8192

# 2. Build and load image
docker build -t candlestick-processor:1.0.0 .
minikube image load candlestick-processor:1.0.0

# 3. Deploy everything
kubectl apply -f kafka-cluster.yaml
kubectl apply -f kubernetes-manifests.yaml

# 4. Wait and test!