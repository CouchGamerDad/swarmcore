# SwarmCore.io deployment notes

This project is ready to deploy as a single Node web service.

## Local run

```bash
npm install
npm start
```

Open this locally:

```txt
http://localhost:3000
```

## Render settings

Build command:

```bash
npm install
```

Start command:

```bash
npm start
```

Health check path:

```txt
/health
```

Environment variables:

```txt
NODE_ENV=production
```

## Important multiplayer note

Keep it on one running instance for the first public test. Socket based games need extra setup before scaling across multiple servers.

## What was cleaned

The deploy copy excludes node_modules and local server log files. Hosting platforms should install dependencies from package.json and package lock during deployment.
