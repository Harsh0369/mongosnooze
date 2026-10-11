# Contributing to MongoSnooze

Thank you for your interest in contributing to **MongoSnooze**! We welcome bug reports, documentation improvements, feature suggestions, and pull requests.

---

## 🛠️ Development Setup

### Prerequisites
- **Node.js**: v18.0.0 or higher
- **npm**: v9.0.0 or higher
- **Git**

### Installation
1. Fork and clone the repository:
   ```bash
   git clone https://github.com/your-username/mongosnooze.git
   cd mongosnooze
   ```
2. Install dependencies:
   ```bash
   npm install
   ```

---

## 🧪 Testing

We use [Vitest](https://vitest.dev/) for fast unit and integration tests.

- Run the full test suite:
  ```bash
  npm test
  ```
- Run tests in watch mode during development:
  ```bash
  npm run test:watch
  ```

---

## 📦 Building

MongoSnooze uses [tsup](https://tsup.egoist.dev/) to produce dual ESM (`dist/index.js`) and CommonJS (`dist/index.cjs`) bundles along with full TypeScript type definitions (`dist/index.d.ts`).

- Build the project:
  ```bash
  npm run build
  ```
- Watch mode for auto-rebuilding:
  ```bash
  npm run dev
  ```

---

## 📝 Pull Request Guidelines

1. **Create a branch** for your change:
   ```bash
   git checkout -b feat/my-new-feature
   ```
2. **Add tests** for any new functionality or bug fixes.
3. **Verify build and tests pass**:
   ```bash
   npm run build
   npm test
   ```
4. **Follow conventional commits**:
   - `feat: ...` for new features
   - `fix: ...` for bug fixes
   - `docs: ...` for documentation
   - `test: ...` for test additions
5. Submit your PR and describe the motivation, changes made, and edge cases handled.

---

## ⚖️ Code of Conduct
Please note that all participants are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
