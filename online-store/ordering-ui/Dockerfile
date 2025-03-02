FROM --platform=$BUILDPLATFORM node:23.8.0-bullseye-slim AS builder

RUN mkdir /project
WORKDIR /project

RUN npm install -g @angular/cli@18

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

EXPOSE 4200

CMD ["ng", "serve", "--host", "0.0.0.0", "--port", "4200", "--configuration", "production", "--allowed-hosts", "order.bteshome.com", "--disable-host-check"]
