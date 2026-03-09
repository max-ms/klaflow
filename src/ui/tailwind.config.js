/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        navy: {
          900: '#1B2559',
          800: '#232D6B',
          700: '#2C377D',
        },
        klaviyo: {
          green: '#27AE60',
          'green-light': '#2ECC71',
          'green-dark': '#1E8449',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
