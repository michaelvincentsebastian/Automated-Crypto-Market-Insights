/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        background: "#050807", // Canvas floor - Pure dark obsidian
        foreground: "#f1ffef", // Crisp light text
        card: {
          DEFAULT: "#0b120e", // Surface tier 1
          foreground: "#f1ffef",
          border: "rgba(26, 46, 34, 0.75)",
          hover: "#0e1713",
        },
        surface: {
          1: "#0b120e",
          2: "#0e1713",
          3: "#13201a",
        },
        primary: {
          DEFAULT: "#00ff88", // Radiant Neon Emerald
          hover: "#00e57a",
          foreground: "#050807",
        },
        secondary: {
          DEFAULT: "#0e1713",
          hover: "#14221c",
          foreground: "#b9cbb9",
        },
        muted: {
          DEFAULT: "#0e1713",
          foreground: "#7e9c8b",
        },
        accent: {
          DEFAULT: "#00ff88",
          foreground: "#050807",
        },
        semantic: {
          up: "#00ff88",
          upBg: "rgba(0, 255, 136, 0.1)",
          down: "#ff3366",
          downBg: "rgba(255, 51, 102, 0.1)",
          neutral: "#7e9c8b",
          cyan: "#00e5ff",
        },
        border: "rgba(26, 46, 34, 0.75)",
        input: "#080d0a",
        ring: "#00ff88",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        display: ["Space Grotesk", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
      boxShadow: {
        "emerald-glow": "0 0 16px rgba(0, 255, 136, 0.15)",
        "emerald-glow-lg": "0 0 24px rgba(0, 255, 136, 0.25)",
        "rose-glow": "0 0 16px rgba(255, 51, 102, 0.15)",
      },
    },
  },
  plugins: [],
};
