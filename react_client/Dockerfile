FROM node:18-alpine

WORKDIR /usr/src/app

COPY ./package.json ./
# Copy application dependency manifests to the container image.
COPY package*.json ./

# Clear npm cache, remove node_modules, and package-lock.json if they exist
RUN npm cache clean --force && rm -rf node_modules package-lock.json

RUN npm install

COPY . .
# Ensure the correct permissions are set for the app directory
RUN chown -R node:node /usr/src/app

# Set the user to "node" to avoid running the container as root
USER node

EXPOSE 8080

CMD [ "npm", "run", "dev" ]