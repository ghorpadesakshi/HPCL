# Use already downloaded bitnami/kafka as base
FROM bitnami/kafka:latest

# Set working directory
WORKDIR /app

# Copy producer code into container
COPY . .

# Default command to run producer (replace with your actual script if needed)
CMD ["bash", "-c", "echo 'Producer container ready'"]
