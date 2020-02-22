use clokwerk::TimeUnits;
use linux_embedded_hal::Serial;
use pms_7003::{OutputFrame, Pms7003Sensor};
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions};
use serde_derive::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

/// Queries air quality sensor and publishes results using MQTT
#[derive(StructOpt)]
#[structopt(version = "1.0")]
struct Opts {
    /// Path to pms-7003 device
    #[structopt(short = "d", long = "device")]
    device: PathBuf,
    /// MQTT broker addreess
    #[structopt(short = "b", long = "broker")]
    broker: String,
    /// MQTT port
    #[structopt(short = "p", long = "port", default_value = "1883")]
    port: u16,
    /// MQTT topic
    #[structopt(short = "t", long = "topic")]
    topic: String,
    /// Interval between queries in seconds
    #[structopt(short = "i", long = "interval", default_value = "60")]
    interval: u32,
    /// Number of queries for a single measurement
    #[structopt(short = "m", long = "measurements", default_value = "10")]
    measurements: usize,
}

fn main() {
    let opts = Opts::from_args();

    let mut mqtt_client = mqtt_connection(&opts);
    println!("Broker connected");

    let device = linux_embedded_hal::Serial::open(&opts.device).unwrap();
    let mut sensor = pms_7003::Pms7003Sensor::new(device);
    println!("Device connected");

    let _ = sensor.active();

    let status = get_air_quality_status(&mut sensor, &opts).unwrap();
    publish_status(&mut mqtt_client, &status, &opts.topic);

    let mut scheduler = clokwerk::Scheduler::new();

    scheduler.every(opts.interval.seconds()).run(move || {
        let status = get_air_quality_status(&mut sensor, &opts).unwrap();
        publish_status(&mut mqtt_client, &status, &opts.topic);
    });

    loop {
        scheduler.run_pending();
        std::thread::sleep(Duration::from_secs(10));
    }
}

#[derive(Debug, Serialize)]
struct AitQualityStatus {
    pm_1_0: u32,
    pm_2_5: u32,
    pm_10: u32,
    timestamp: chrono::DateTime<chrono::Local>,
}

fn get_air_quality_status(
    sensor: &mut Pms7003Sensor<Serial>,
    opts: &Opts,
) -> Result<AitQualityStatus, pms_7003::Error> {
    println!("Waking sensor");
    loop {
        sensor.wake()?;
        if sensor.read().is_ok() {
            break;
        }
    }

    println!("Warming up");
    let warmup_start = std::time::Instant::now();
    loop {
        let _ = sensor.read();
        if (std::time::Instant::now() - warmup_start) > Duration::from_secs(30) {
            break;
        }
    }

    println!("Reading measurements");
    let mut measurements = std::vec::Vec::<OutputFrame>::new();

    while measurements.len() < opts.measurements {
        match sensor.read() {
            Ok(measurement) => {
                println!("{:?}", measurement);
                measurements.push(measurement);
            }
            Err(e) => print!("{:?}", e),
        }
    }

    println!("Going to sleep");
    let _ = sensor.sleep();

    Ok(status_from_measurements(&measurements))
}

fn publish_status(mqtt_client: &mut MqttClient, status: &AitQualityStatus, topic: &str) {
    mqtt_client
        .publish(
            format!("{}/status", topic),
            QoS::AtLeastOnce,
            true,
            serde_json::to_string_pretty(status).unwrap(),
        )
        .unwrap();
    mqtt_client
        .publish(
            format!("{}/pm1_0", topic),
            QoS::AtLeastOnce,
            true,
            format!("{}", status.pm_1_0),
        )
        .unwrap();
    mqtt_client
        .publish(
            format!("{}/pm2_5", topic),
            QoS::AtLeastOnce,
            true,
            format!("{}", status.pm_2_5),
        )
        .unwrap();
    mqtt_client
        .publish(
            format!("{}/pm10", topic),
            QoS::AtLeastOnce,
            true,
            format!("{}", status.pm_10),
        )
        .unwrap();

    println!("Published: {:?}", status);
}

fn status_from_measurements(measurements: &[OutputFrame]) -> AitQualityStatus {
    AitQualityStatus {
        pm_1_0: measurements.iter().map(|m| m.pm1_0 as u32).sum::<u32>()
            / measurements.len() as u32,
        pm_2_5: measurements.iter().map(|m| m.pm2_5 as u32).sum::<u32>()
            / measurements.len() as u32,
        pm_10: measurements.iter().map(|m| m.pm10 as u32).sum::<u32>() / measurements.len() as u32,
        timestamp: chrono::Local::now(),
    }
}

fn mqtt_connection(opts: &Opts) -> MqttClient {
    let reconnection_options = ReconnectOptions::Always(10);
    let mqtt_options = MqttOptions::new("aqbc", &opts.broker, opts.port)
        .set_keep_alive(10)
        .set_inflight(3)
        .set_request_channel_capacity(3)
        .set_reconnect_opts(reconnection_options)
        .set_clean_session(false);

    let (mqtt_client, _notifications) = MqttClient::start(mqtt_options).unwrap();
    mqtt_client
}
