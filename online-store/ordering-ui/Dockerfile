FROM --platform=$BUILDPLATFORM node:23.8.0-bullseye-slim AS builder

RUN mkdir /project
WORKDIR /project

RUN npm install -g @angular/cli@19

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

EXPOSE 80

CMD ["ng", "serve", "--host", "0.0.0.0", "--port", "80", "--configuration", "development"]
