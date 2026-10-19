#![windows_subsystem = "windows"]

mod app;
mod auth;
mod config;
mod model;
mod secure;
mod store;
mod worker;

use eframe::egui;

fn main() -> eframe::Result<()> {
    let options = |w: f32, h: f32| eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("NT Notes")
            .with_inner_size([w, h])
            .with_min_inner_size([560.0, 380.0]),
        ..Default::default()
    };

    match config::load_config() {
        Ok(loaded) => eframe::run_native(
            "NT Notes",
            options(1000.0, 650.0),
            Box::new(move |cc| Ok(Box::new(app::NotesApp::new(cc, loaded)))),
        ),
        Err(message) => eframe::run_native(
            "NT Notes",
            options(560.0, 300.0),
            Box::new(move |_cc| Ok(Box::new(app::FatalApp(message)))),
        ),
    }
}
