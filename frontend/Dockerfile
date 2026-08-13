FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .

# Vite inlines VITE_* vars into the built JS at build time, not at
# container runtime, so they have to arrive as build args here rather than
# as plain `environment:` entries in docker-compose.yaml. Changing one of
# these requires rebuilding the image, not just restarting the container.
ARG VITE_MEDIAMTX_WHEP_BASE
ARG VITE_SESSION_SSE_BASE
ARG VITE_SESSION_API_BASE
ENV VITE_MEDIAMTX_WHEP_BASE=$VITE_MEDIAMTX_WHEP_BASE \
    VITE_SESSION_SSE_BASE=$VITE_SESSION_SSE_BASE \
    VITE_SESSION_API_BASE=$VITE_SESSION_API_BASE
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
