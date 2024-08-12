# Use the official Node.js image as the base image
FROM node:14

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the remaining application files
COPY . .

# Expose the port that your Node.js application listens on
EXPOSE 3000

# Start the Node.js application
CMD ["npm", "start"]
