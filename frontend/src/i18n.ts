import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import zh from './locales/zh.json'
import en from './locales/en.json'

const LANG_KEY = 'lang'

function getInitialLang(): string {
  const stored = localStorage.getItem(LANG_KEY)
  if (stored === 'zh' || stored === 'en') return stored
  return navigator.language.startsWith('zh') ? 'zh' : 'en'
}

i18n.use(initReactI18next).init({
  resources: {
    zh: { translation: zh },
    en: { translation: en },
  },
  lng: getInitialLang(),
  fallbackLng: 'zh',
  interpolation: {
    escapeValue: false,
  },
})

export default i18n
