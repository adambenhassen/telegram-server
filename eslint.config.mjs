import js from '@eslint/js';
import playwright from 'eslint-plugin-playwright';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default [
  {
    ignores: ['node_modules/**', 'test-results/**', 'playwright-report/**'],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['test/e2e/**/*.ts'],
    plugins: { playwright },
    languageOptions: {
      globals: { ...globals.node },
    },
    rules: { ...playwright.configs['flat/recommended'].rules },
  },
];
