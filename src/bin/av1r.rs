// Copyright (c) 2017-2023, The rav1e contributors. All rights reserved
//
// This source code is subject to the terms of the BSD 2 Clause License and
// the Alliance for Open Media Patent License 1.0. If the BSD 2 Clause License
// was not distributed with this source code in the LICENSE file, you can
// obtain it at www.aomedia.org/license/software. If the Alliance for Open
// Media Patent License 1.0 was not distributed with this source code in the
// PATENTS file, you can obtain it at www.aomedia.org/license/patent.

#![feature(never_type, async_closure)]

#[macro_use]
extern crate log;

mod common;
mod decoder;
mod error;
#[cfg(feature = "serialize")]
mod kv;
mod muxer;
mod stats;

use crate::common::*;
use crate::error::*;
use crate::stats::*;
use rav1e::config::CpuFeatureLevel;
use rav1e::prelude::*;
use std::fs;

use crate::decoder::{Decoder, FrameBuilder, VideoDetails};
use crate::muxer::*;
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::routing::{get, post};
use axum::{body, Json, Router};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Cursor, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;

impl<T: Pixel> FrameBuilder<T> for FrameSender<T> {
  fn new_frame(&self) -> Frame<T> {
    FrameSender::new_frame(self)
  }
}

struct Source<D: Decoder> {
  limit: usize,
  count: usize,
  input: D,
}

impl<D: Decoder> Source<D> {
  fn new(limit: usize, input: D) -> Self {
    Self { limit, input, count: 0 }
  }

  fn read_frame<T: Pixel>(
    &mut self, send_frame: &mut FrameSender<T>, video_info: VideoDetails,
  ) -> bool {
    if self.limit != 0 && self.count == self.limit {
      return false;
    }

    match self.input.read_frame(send_frame, &video_info) {
      Ok(frame) => {
        self.count += 1;
        let _ = send_frame.send(frame);
        true
      }
      _ => false,
    }
  }
}

fn do_encode<T: Pixel, D: Decoder>(
  cfg: Config, verbose: Verboseness, mut progress: ProgressInfo,
  output: &mut dyn Muxer, mut source: Source<D>,
  mut y4m_enc: Option<y4m::Encoder<Box<dyn Write + Send>>>,
  broadcast: &mut watch::Sender<Status>, metrics_enabled: MetricsEnabled,
) -> Result<(), CliError> {
  let (mut send_frame, mut receive_packet) =
    cfg.new_channel::<T>().map_err(|e| e.context("Invalid setup"))?;

  let y4m_details = source.input.get_video_details();

  crossbeam::thread::scope(move |s| -> Result<(), CliError> {
    let send_frames =
      s.spawn(
        move |_| {
          while source.read_frame(&mut send_frame, y4m_details) {}
        },
      );

    let receive_packets = s.spawn(move |_| -> Result<(), CliError> {
      for pkt in receive_packet.iter() {
        output.write_frame(
          pkt.input_frameno,
          pkt.data.as_ref(),
          pkt.frame_type,
        );
        output.flush().unwrap();
        if let (Some(ref mut y4m_enc_uw), Some(ref rec)) =
          (y4m_enc.as_mut(), &pkt.rec)
        {
          write_y4m_frame(y4m_enc_uw, rec, y4m_details);
        }
        let summary = build_frame_summary(
          pkt,
          y4m_details.bit_depth,
          y4m_details.chroma_sampling,
          metrics_enabled.into(),
        );

        if true || verbose != Verboseness::Quiet {
          progress.add_frame(summary.clone());
          let _ =
            broadcast.send(Status::Frame(progress.frames_encoded())).unwrap();
          if verbose == Verboseness::Verbose {
            info!("{} - {}", summary, progress);
          } else {
            // Print a one-line progress indicator that overrides itself with every update
            // eprint!("\r{}                    ", progress);
          };
        }
      }

      if verbose != Verboseness::Quiet {
        if verbose == Verboseness::Verbose {
          // Clear out the temporary progress indicator
          eprint!("\r");
        }
        progress.print_summary(verbose == Verboseness::Verbose);
      }

      // receive_packet.result()
      Ok(())
    });

    send_frames.join().expect("The send frames thread panicked");
    receive_packets.join().expect("The receive packets thread panicked")?;

    Ok(())
  })
  .unwrap()
}

use anyhow::Result;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::{stream, Stream};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task;

async fn service(
  req: Request, broadcast: watch::Sender<Status>,
) -> Result<impl IntoResponse, String> {
  let _ = broadcast.send_replace(Status::Frame(0));

  let bytes = body::to_bytes(req.into_body(), usize::MAX).await.unwrap();

  let input = Cursor::new(bytes);
  let mut output = Vec::with_capacity(4096);
  let mut muxer = IvfMuxer { output };

  let (tx, rx) = oneshot::channel();
  task::spawn_blocking(move || {
    let _ = tx.send(
      run(Box::new(input), &mut muxer, EncoderOptions::default(), broadcast)
        .map(|_| muxer),
    );
  });
  let muxer = rx.await.unwrap().map_err(|e| format!("{:?}", e))?;



  Ok(Bytes::from(muxer.output))
}

#[derive(Debug, Serialize, Copy, Clone)]
pub enum Status {
  Frame(usize),
  Finish,
  None,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  init_logger();

  #[cfg(feature = "tracing")]
  let (chrome_layer, _guard) =
    tracing_chrome::ChromeLayerBuilder::new().build();

  #[cfg(feature = "tracing")]
  {
    use tracing_subscriber::layer::subscriberext;
    tracing::subscriber::set_global_default(
      tracing_subscriber::registry().with(chrome_layer),
    )
    .unwrap();
  }

  for i in 0..32 {
    let (tx, mut rx) = watch::channel(Status::None);
    tokio::spawn(async move {
      axum::serve(
        TcpListener::bind(format!("0.0.0.0:3{i:03}")).await.unwrap(),
        Router::new()
          .route("/encode", post(move |req: Request| service(req, tx)))
          .route(
            "/status",
            get(move || async move {
              let status = *rx.borrow_and_update();
              println!("STATUS: {status:?}");
              Json(status)
            }),
          ),
      )
      .await
      .unwrap();
    });
  }

  loop {}

  Ok(())
}

fn init_logger() {
  use std::str::FromStr;
  fn level_colored(l: log::Level) -> console::StyledObject<&'static str> {
    use console::style;
    use log::Level;
    match l {
      Level::Trace => style("??").dim(),
      Level::Debug => style("? ").dim(),
      Level::Info => style("> ").green(),
      Level::Warn => style("! ").yellow(),
      Level::Error => style("!!").red(),
    }
  }

  let level = std::env::var("RAV1E_LOG")
    .ok()
    .and_then(|l| log::LevelFilter::from_str(&l).ok())
    .unwrap_or(log::LevelFilter::Info);

  fern::Dispatch::new()
    .format(move |out, message, record| {
      out.finish(format_args!(
        "{level} {message}",
        level = level_colored(record.level()),
        message = message,
      ));
    })
    // set the default log level. to filter out verbose log messages from dependencies, set
    // this to Warn and overwrite the log level for your crate.
    .level(log::LevelFilter::Warn)
    // change log levels for individual modules. Note: This looks for the record's target
    // field which defaults to the module path but can be overwritten with the `target`
    // parameter:
    // `info!(target="special_target", "This log message is about special_target");`
    .level_for("rav1e", level)
    .level_for("rav1e_ch", level)
    // output to stdout
    .chain(std::io::stderr())
    .apply()
    .unwrap();
}

cfg_if::cfg_if! {
  if #[cfg(any(target_os = "windows", target_arch = "wasm32"))] {
    fn print_rusage() {
      eprintln!("Resource usage reporting is not currently supported on this platform");
    }
  } else {
    fn print_rusage() {
      // SAFETY: This uses an FFI, it is safe because we call it correctly.
      let (utime, stime, maxrss) = unsafe {
        let mut usage = std::mem::zeroed();
        let _ = libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        (usage.ru_utime, usage.ru_stime, usage.ru_maxrss)
      };
      eprintln!(
        "user time: {} s",
        utime.tv_sec as f64 + utime.tv_usec as f64 / 1_000_000f64
      );
      eprintln!(
        "system time: {} s",
        stime.tv_sec as f64 + stime.tv_usec as f64 / 1_000_000f64
      );
      eprintln!("maximum rss: {} KB", maxrss);
    }
  }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verboseness {
  #[default]
  Quiet,
  Normal,
  Verbose,
}

#[derive(
  Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
pub enum MetricsEnabled {
  /// Don't calculate any metrics.
  #[default]
  None,
  /// Calculate the PSNR of each plane, but no other metrics.
  Psnr,
  /// Calculate all implemented metrics. Currently implemented metrics match what is available via AWCY.
  All,
}

impl Into<stats::MetricsEnabled> for MetricsEnabled {
  fn into(self) -> stats::MetricsEnabled {
    match self {
      Self::None => stats::MetricsEnabled::None,
      Self::Psnr => stats::MetricsEnabled::Psnr,
      Self::All => stats::MetricsEnabled::All,
    }
  }
}

#[derive(Default, Serialize, Deserialize)]
pub struct EncoderOptions {
  pub enc: EncoderConfig,
  pub limit: usize,
  pub color_range_specified: bool,
  pub override_time_base: bool,
  pub skip: usize,
  pub verbose: Verboseness,
  pub benchmark: bool,
  pub threads: usize,
  pub metrics_enabled: MetricsEnabled,
  pub photon_noise: u8,
  #[cfg(feature = "unstable")]
  pub slots: usize,
  pub force_highbitdepth: bool,
}

fn run(
  input: Box<dyn Read + Send>, output: &mut (dyn Muxer + Send),
  mut cli: EncoderOptions, mut tx: watch::Sender<Status>,
) -> Result<(), error::CliError> {
  // Maximum frame size by specification + maximum y4m header
  let limit = y4m::Limits {
    // Use saturating operations to gracefully handle 32-bit architectures
    bytes: 64usize
      .saturating_mul(64)
      .saturating_mul(4096)
      .saturating_mul(2304)
      .saturating_add(1024),
  };
  let mut y4m_dec = match y4m::Decoder::new_with_limits(input, limit) {
        Err(e) => {
            return Err(CliError::new(match e {
                y4m::Error::ParseError(_) => {
                    "Could not parse input video. Is it a y4m file?"
                }
                y4m::Error::IoError(_) => {
                    "Could not read input file. Check that the path is correct and you have read permissions."
                }
                y4m::Error::UnknownColorspace => {
                    "Unknown colorspace or unsupported bit depth."
                }
                y4m::Error::OutOfMemory => "The video's frame size exceeds the limit.",
                y4m::Error::EOF => "Unexpected end of input.",
                y4m::Error::BadInput => "Bad y4m input parameters provided.",
            }))
        }
        Ok(d) => d,
    };
  let video_info = y4m_dec.get_video_details();
  // let y4m_enc = cli.io.rec.map(|rec| {
  //   y4m::encode(
  //     video_info.width,
  //     video_info.height,
  //     y4m::Ratio::new(
  //       video_info.time_base.den as usize,
  //       video_info.time_base.num as usize,
  //     ),
  //   )
  //   .with_colorspace(y4m_dec.get_colorspace())
  //   .with_pixel_aspect(y4m::Ratio {
  //     num: video_info.sample_aspect_ratio.num as usize,
  //     den: video_info.sample_aspect_ratio.den as usize,
  //   })
  //   .write_header(rec)
  //   .unwrap()
  // });
  let mut y4m_enc = None;

  match video_info.bit_depth {
    8 | 10 | 12 => {}
    _ => return Err(CliError::new("Unsupported bit depth")),
  }

  cli.enc.width = video_info.width;
  cli.enc.height = video_info.height;
  cli.enc.bit_depth = video_info.bit_depth;
  cli.enc.sample_aspect_ratio = video_info.sample_aspect_ratio;
  cli.enc.chroma_sampling = video_info.chroma_sampling;
  cli.enc.chroma_sample_position = video_info.chroma_sample_position;

  // If no pixel range is specified via CLI, assume limited,
  // as it is the default for the Y4M format.
  if !cli.color_range_specified {
    cli.enc.pixel_range = PixelRange::Limited;
  }

  if !cli.override_time_base {
    cli.enc.time_base = video_info.time_base;
  }

  if cli.photon_noise > 0 && cli.enc.film_grain_params.is_none() {
    cli.enc.film_grain_params = Some(vec![generate_photon_noise_params(
      0,
      u64::MAX,
      NoiseGenArgs {
        iso_setting: cli.photon_noise as u32 * 100,
        width: video_info.width as u32,
        height: video_info.height as u32,
        transfer_function: if cli.enc.is_hdr() {
          TransferFunction::SMPTE2084
        } else {
          TransferFunction::BT1886
        },
        chroma_grain: false,
        random_seed: None,
      },
    )]);
  }

  let rc = RateControlConfig::new();
  let cfg = Config::new()
    .with_encoder_config(cli.enc.clone())
    .with_threads(cli.threads)
    .with_rate_control(rc)
    .with_parallel_gops(cli.slots);

  output.write_header(
    video_info.width,
    video_info.height,
    cli.enc.time_base.den as usize,
    cli.enc.time_base.num as usize,
  );

  let tiling =
    cfg.tiling_info().map_err(|e| e.context("Invalid configuration"))?;
  if true || cli.verbose != Verboseness::Quiet {
    info!("CPU Feature Level: {}", CpuFeatureLevel::default());

    info!(
      "Using y4m decoder: {}x{}p @ {}/{} fps, {}, {}-bit",
      video_info.width,
      video_info.height,
      video_info.time_base.den,
      video_info.time_base.num,
      video_info.chroma_sampling,
      video_info.bit_depth
    );
    info!("Encoding settings: {}", cli.enc);

    if tiling.tile_count() == 1 {
      info!("Using 1 tile");
    } else {
      info!(
        "Using {} tiles ({}x{})",
        tiling.tile_count(),
        tiling.cols,
        tiling.rows
      );
    }
  }

  let progress = ProgressInfo::new(
    Rational { num: video_info.time_base.den, den: video_info.time_base.num },
    if cli.limit == 0 { None } else { Some(cli.limit) },
    cli.metrics_enabled.into(),
  );

  for _ in 0..cli.skip {
    match y4m_dec.read_frame() {
      Ok(f) => f,
      Err(_) => {
        return Err(CliError::new("Skipped more frames than in the input"))
      }
    };
  }

  let source = Source::new(cli.limit, y4m_dec);

  if video_info.bit_depth == 8 && !cli.force_highbitdepth {
    do_encode::<u8, y4m::Decoder<Box<dyn Read + Send>>>(
      cfg,
      cli.verbose,
      progress,
      &mut *output,
      source,
      y4m_enc,
      &mut tx,
      cli.metrics_enabled,
    )?
  } else {
    do_encode::<u16, y4m::Decoder<Box<dyn Read + Send>>>(
      cfg,
      cli.verbose,
      progress,
      &mut *output,
      source,
      y4m_enc,
      &mut tx,
      cli.metrics_enabled,
    )?
  }
  if cli.benchmark {
    print_rusage();
  }

  let _ = tx.send(Status::Finish);

  Ok(())
}
