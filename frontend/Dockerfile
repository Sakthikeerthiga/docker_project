# Dockerfile for Frontend Service

# Use a lightweight web server image
FROM nginx:latest

# Copy the HTML file to the default Nginx directory
COPY index.html /usr/share/nginx/html/index.html

# Expose port 80 for HTTP traffic
EXPOSE 80

# Start Nginx server
CMD ["nginx", "-g", "daemon off;"]