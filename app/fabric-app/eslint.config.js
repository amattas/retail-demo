import js from '@eslint/js';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    ignores: [
      'dist',
      '**/dist',
      'node_modules',
      '.rush',
      'rush-logs',
      'rayfin/.temp',
    ],
  },
  {
    files: ['**/*.{ts,tsx}'],
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.browser,
    },
    plugins: {
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': [
        'warn',
        { allowConstantExport: true },
      ],
      '@typescript-eslint/no-unused-vars': [
        'warn',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
    },
  },
  {
    // Rayfin backend code (user data functions, data model).
    //
    // `no-deprecated` needs type information, so this block turns on the
    // project service rather than adopting all of `recommendedTypeChecked` —
    // type-aware linting is slower and the extra rules are a separate decision.
    //
    // It surfaces the SDK's own `@deprecated` markers as build errors, which is
    // how `ctx.getToken(...)` / `ctx.getSecret(...)` are steered towards
    // `ctx.Tokens.<Audience>` / `ctx.Secrets.<NAME>`.
    files: ['rayfin/**/*.ts'],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-deprecated': 'error',
    },
  }
);
