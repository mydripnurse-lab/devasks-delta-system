/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    externalDir: true, // permite importar ../services/*
  },
};

module.exports = nextConfig;