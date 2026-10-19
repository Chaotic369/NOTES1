//! Background worker. Owns the session, the local note store and the settings file.
//! The UI talks to it through `Cmd` (UI -> worker) and `Evt` (worker -> UI) channels and
//! never blocks. Network calls run on short-lived helper threads that report back to the
//! worker through the same command channel.
//!
//! PHASE 2 hooks (not implemented yet):
//!  * `Worker::on_local_change`   - enqueue the note for upload (offline queue).
//!  * `Worker::begin_session`     - start the sync/streaming threads for the user.
//!  * `Worker::set_status`        - report Synced / Syncing / Offline.
//!  * `auth::call_with_refresh`   - authenticated REST calls with 401 -> refresh -> retry.
//!  * `NoteStore::merge_remote`   - "newest updatedAt wins" merge of remote notes.

use crate::auth::{self, AuthError, Refreshed, Session};
use crate::config::{self, AppConfig, Settings};
use crate::model::Note;
use crate::secure::{self, StoredSession};
use crate::store::NoteStore;
use eframe::egui;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const REFRESH_EVERY: Duration = Duration::from_secs(50 * 60);
const RETRY_AFTER: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(dead_code)] // Synced/Syncing are used by Phase 2
pub enum SyncStatus {
    Synced,
    Syncing,
    Offline,
}

/// UI -> worker.
pub enum Cmd {
    SignIn { email: String, password: String },
    SignUp { email: String, password: String },
    ResetPassword { email: String },
    GoogleSignIn,
    CancelGoogle,
    Logout,
    SaveNote(Note),
    SetTheme(bool),
    Quit,
    // internal (sent by helper threads)
    AuthDone { result: Result<Session, AuthError>, google: bool },
    RefreshDone(Result<Refreshed, AuthError>),
}

/// Worker -> UI.
pub enum Evt {
    NoSession,
    SignedIn { email: String, notes: Vec<Note> },
    SignedOut { message: Option<String> },
    AuthError(String),
    AuthCancelled,
    Info(String),
    Status(SyncStatus),
    Error(String),
}

#[derive(Clone)]
struct Notifier {
    tx: Sender<Evt>,
    ctx: egui::Context,
}

impl Notifier {
    fn send(&self, e: Evt) {
        let _ = self.tx.send(e);
        self.ctx.request_repaint();
    }
}

struct ActiveUser {
    session: Session,
    store: NoteStore,
    next_refresh: Instant,
}

struct Worker {
    cfg: AppConfig,
    settings: Settings,
    settings_fresh: bool,
    rx: Receiver<Cmd>,
    self_tx: Sender<Cmd>,
    out: Notifier,
    user: Option<ActiveUser>,
    google_cancel: Arc<AtomicBool>,
    google_running: bool,
    refreshing: bool,
}

pub fn spawn(
    cfg: AppConfig,
    settings: Settings,
    settings_fresh: bool,
    ctx: egui::Context,
) -> (Sender<Cmd>, Receiver<Evt>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (evt_tx, evt_rx) = mpsc::channel();
    let worker = Worker {
        cfg,
        settings,
        settings_fresh,
        rx: cmd_rx,
        self_tx: cmd_tx.clone(),
        out: Notifier { tx: evt_tx, ctx },
        user: None,
        google_cancel: Arc::new(AtomicBool::new(false)),
        google_running: false,
        refreshing: false,
    };
    let handle = thread::Builder::new()
        .name("ntnotes-worker".into())
        .spawn(move || worker.run())
        .expect("failed to start worker thread");
    (cmd_tx, evt_rx, handle)
}

impl Worker {
    fn run(mut self) {
        if self.settings_fresh {
            self.save_settings();
        }
        self.restore_session();
        loop {
            match self.rx.recv_timeout(Duration::from_secs(5)) {
                Ok(cmd) => {
                    if !self.handle(cmd) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.tick();
        }
    }

    // ------------------------------------------------------------ commands

    /// Returns false when the worker should stop.
    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::SignIn { email, password } => {
                self.spawn_auth(false, move |cfg| auth::sign_in(cfg, &email, &password))
            }
            Cmd::SignUp { email, password } => {
                self.spawn_auth(false, move |cfg| auth::sign_up(cfg, &email, &password))
            }
            Cmd::ResetPassword { email } => {
                let cfg = self.cfg.clone();
                let out = self.out.clone();
                thread::spawn(move || match auth::send_password_reset(&cfg, &email) {
                    Ok(()) => out.send(Evt::Info(format!(
                        "Password reset email sent to {email}. Check your inbox."
                    ))),
                    Err(e) => out.send(Evt::AuthError(e.to_string())),
                });
            }
            Cmd::GoogleSignIn => {
                if self.google_running {
                    return true;
                }
                self.google_running = true;
                self.google_cancel.store(false, Ordering::SeqCst);
                let cancel = self.google_cancel.clone();
                self.spawn_auth(true, move |cfg| auth::google_sign_in(cfg, &cancel));
            }
            Cmd::CancelGoogle => self.google_cancel.store(true, Ordering::SeqCst),
            Cmd::Logout => self.sign_out(None),
            Cmd::SaveNote(note) => self.save_note(note),
            Cmd::SetTheme(dark) => {
                self.settings.dark_mode = dark;
                self.save_settings();
            }
            Cmd::Quit => return false,
            Cmd::AuthDone { result, google } => self.auth_done(result, google),
            Cmd::RefreshDone(result) => self.refresh_done(result),
        }
        true
    }

    fn spawn_auth(
        &mut self,
        google: bool,
        f: impl FnOnce(&AppConfig) -> Result<Session, AuthError> + Send + 'static,
    ) {
        let cfg = self.cfg.clone();
        let tx = self.self_tx.clone();
        thread::spawn(move || {
            let result = f(&cfg);
            let _ = tx.send(Cmd::AuthDone { result, google });
        });
    }

    fn auth_done(&mut self, result: Result<Session, AuthError>, google: bool) {
        if google {
            self.google_running = false;
        }
        match result {
            Ok(session) => {
                self.remember(&session);
                if self.settings.last_email != session.email {
                    self.settings.last_email = session.email.clone();
                    self.save_settings();
                }
                self.begin_session(session, Instant::now() + REFRESH_EVERY);
            }
            Err(AuthError::Cancelled) => self.out.send(Evt::AuthCancelled),
            Err(e) => self.out.send(Evt::AuthError(e.to_string())),
        }
    }

    fn refresh_done(&mut self, result: Result<Refreshed, AuthError>) {
        self.refreshing = false;
        match result {
            Ok(r) => {
                let mut to_store = None;
                if let Some(user) = self.user.as_mut() {
                    if user.session.uid == r.uid {
                        let lifetime = Duration::from_secs(r.expires_in.saturating_sub(300).max(30));
                        user.next_refresh = Instant::now() + lifetime.min(REFRESH_EVERY);
                        user.session.apply(r);
                        to_store = Some(user.session.clone());
                    }
                }
                if let Some(s) = to_store {
                    self.remember(&s);
                }
            }
            Err(e) if e.is_session_dead() => {
                self.sign_out(Some("Your session has expired. Please sign in again.".into()));
            }
            Err(_) => {
                // Offline or a temporary server problem: keep working locally, retry soon.
                if let Some(user) = self.user.as_mut() {
                    user.next_refresh = Instant::now() + RETRY_AFTER;
                }
            }
        }
    }

    // ------------------------------------------------------------ session

    fn restore_session(&mut self) {
        match secure::load() {
            Some(s) if !s.uid.is_empty() && !s.refresh_token.is_empty() => {
                // Open instantly with local data; the token is refreshed in the background.
                let session = Session {
                    uid: s.uid,
                    email: s.email,
                    id_token: String::new(),
                    refresh_token: s.refresh_token,
                };
                self.begin_session(session, Instant::now());
            }
            _ => self.out.send(Evt::NoSession),
        }
    }

    fn begin_session(&mut self, session: Session, next_refresh: Instant) {
        let (store, warning) = NoteStore::open(config::notes_file(&session.uid));
        let notes = store.all();
        let email = session.email.clone();
        self.user = Some(ActiveUser { session, store, next_refresh });
        self.out.send(Evt::SignedIn { email, notes });
        if let Some(w) = warning {
            self.out.send(Evt::Error(w));
        }
        // PHASE 2: start the sync engine for this user here.
        self.set_status(SyncStatus::Offline);
    }

    fn sign_out(&mut self, message: Option<String>) {
        self.user = None;
        secure::clear();
        self.out.send(Evt::SignedOut { message });
    }

    fn remember(&self, s: &Session) {
        let ok = secure::save(&StoredSession {
            uid: s.uid.clone(),
            email: s.email.clone(),
            refresh_token: s.refresh_token.clone(),
        });
        if !ok {
            self.out.send(Evt::Error("Could not store the login on this PC; you will have to sign in again next time.".into()));
        }
    }

    fn tick(&mut self) {
        if self.refreshing {
            return;
        }
        let Some(user) = &self.user else { return };
        if Instant::now() < user.next_refresh {
            return;
        }
        let refresh_token = user.session.refresh_token.clone();
        self.refreshing = true;
        let cfg = self.cfg.clone();
        let tx = self.self_tx.clone();
        thread::spawn(move || {
            let _ = tx.send(Cmd::RefreshDone(auth::refresh(&cfg, &refresh_token)));
        });
    }

    // ------------------------------------------------------------ notes / settings

    fn save_note(&mut self, note: Note) {
        let Some(user) = self.user.as_mut() else { return };
        let result = user.store.upsert_local(note.clone());
        if let Err(e) = result {
            self.out.send(Evt::Error(format!("Could not save note to disk: {e}")));
        }
        self.on_local_change(&note);
    }

    /// PHASE 2: queue `note` for upload to users/{uid}/notes/{id}.
    fn on_local_change(&mut self, _note: &Note) {}

    fn set_status(&self, status: SyncStatus) {
        self.out.send(Evt::Status(status));
    }

    fn save_settings(&self) {
        if let Err(e) = self.settings.save() {
            self.out.send(Evt::Error(format!("Could not save settings: {e}")));
        }
    }
}
