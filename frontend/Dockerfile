# Build stage
FROM node:18-alpine AS build
WORKDIR /app
COPY package*.json /app/
RUN npm cache clean --force && \
    npm install
ENV NODE_ENV=production
COPY . /app
RUN npm run build

# Production stage
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]