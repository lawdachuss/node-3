/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./router/view/templates/**/*.html'],
  darkMode: 'class',
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
