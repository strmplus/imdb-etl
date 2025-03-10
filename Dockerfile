ARG NODE_VERSION=22.14.0

FROM node:${NODE_VERSION}-alpine AS development
WORKDIR /usr/src/app
COPY --chown=node:node package*.json ./
RUN npm ci -f
COPY --chown=node:node . .
USER node

FROM node:${NODE_VERSION}-alpine AS build
WORKDIR /usr/src/app
COPY --chown=node:node package*.json ./
COPY --chown=node:node --from=development /usr/src/app/node_modules ./node_modules
COPY --chown=node:node . .
RUN npm run build && npm ci -f --only=production && npm cache clean --force
USER node

FROM node:${NODE_VERSION}-alpine AS production
WORKDIR /usr/src/app
ENV NODE_ENV production
COPY --chown=node:node --from=build /usr/src/app/node_modules ./node_modules
COPY --chown=node:node --from=build /usr/src/app/dist ./dist

CMD ["node", "dist/main"]
