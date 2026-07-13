/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        odyn: {
          dark: "#0a0b10",
          card: "#12141c",
          teal: "#00f5ff",
          green: "#00ff88",
          red: "#ff3366",
          gray: "#8f9bb3",
        }
      }
    },
  },
  plugins: [],
}
