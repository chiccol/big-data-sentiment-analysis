# Stage 1: Build the React app
FROM node:20-alpine as build

WORKDIR /app

# Copy package.json and yarn.lock
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install

ENV NODE_ENV=production

# Copy the rest of the application files
COPY . /app

# Build the React app
RUN yarn build

# Stage 2: Serve the React app using a lightweight web server
FROM nginx:alpine

COPY --from=build /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
