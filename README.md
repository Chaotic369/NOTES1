# NT Notes (Phase 1)

Native Windows notes app: Rust + eframe/egui, no webview. Phase 1 = Firebase auth + local offline notes.

## Build
    cargo build --release
Output: `target/release/ntnotes.exe` (also built by `.github/workflows/build.yml` on windows-latest).

## Configure
Put `config.json` next to the exe. If it is missing, the app creates it and shows a notice.
Replace `googleClientId` / `googleClientSecret` with your Google OAuth **Desktop app** client values
(Google Cloud Console > Credentials). Email/password works without them.
Firebase console: enable Email/Password and Google providers.

## Where data lives
- Notes: `%LOCALAPPDATA%\NTNotes\NTNotes\data\users\<uid>\notes.json`
- Settings (deviceId, theme): `%APPDATA%\NTNotes\NTNotes\config\settings.json`
- Refresh token: Windows Credential Manager (`NTNotes/firebase-session`), file fallback in the config folder.

## Phase 2 hooks
`worker.rs`: `on_local_change`, `begin_session`, `set_status`. `auth.rs`: `call_with_refresh`.
`store.rs`: `merge_remote` (newest updatedAt wins).
