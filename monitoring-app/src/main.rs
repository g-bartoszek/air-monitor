use linux_embedded_hal::Serial;
use pms_7003::{OutputFrame, Pms7003Sensor};
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions};
use serde_derive::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use tokio::macros::support::Future;

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
#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

    let mqtt_client = std::sync::Arc::new(std::sync::Mutex::new(mqtt_connection(&opts)));
    println!("MQTT client connected");

    let device = linux_embedded_hal::Serial::open(&opts.device).unwrap();
    let mut sensor = pms_7003::Pms7003Sensor::new(device);
    println!("Pms7003 connected");

    let _ = sensor.active();

    let mut interval = tokio::time::interval(Duration::from_secs(opts.interval.into()));

    let i2c_bus = linux_embedded_hal::I2cdev::new("/dev/i2c-1").unwrap();
    let mut bme280 = bme280::BME280::new_primary(i2c_bus, linux_embedded_hal::Delay);
    bme280.init().unwrap();
    println!("Bme280 connected");

    loop {
        interval.tick().await;

        let pollution_task = tokio::spawn(pollution_task(
            sensor,
            opts.measurements,
            mqtt_client.clone(),
            opts.topic.clone(),
        ));

        let temperature_task = tokio::spawn(temperature_task(
            bme280,
            mqtt_client.clone(),
            opts.topic.clone(),
        ));

        sensor = pollution_task.await.unwrap();
        bme280 = temperature_task.await.unwrap();
    }
}

type Pms7003 = Pms7003Sensor<Serial>;
type Bme = bme280::BME280<linux_embedded_hal::I2cdev, linux_embedded_hal::Delay>;

fn pollution_task(
    mut sensor: Pms7003,
    num_of_measurements: usize,
    mqtt_client: std::sync::Arc<std::sync::Mutex<MqttClient>>,
    topic: String,
) -> impl Future<Output = Pms7003> {
    async move {
        let status = get_air_quality_status(&mut sensor, num_of_measurements).unwrap();
        publish_status(&mut mqtt_client.lock().unwrap(), &status, &topic);
        sensor
    }
}

fn temperature_task(
    mut bme280: Bme,
    mqtt_client: std::sync::Arc<std::sync::Mutex<MqttClient>>,
    topic: String,
) -> impl Future<Output = Bme> {
    async move {
        let measurement = bme280.measure().unwrap();

        let mut client_lock = mqtt_client.lock().unwrap();
        publish(
            &mut client_lock,
            &format!("{}/humidity", topic),
            &format!("{:.2}%", measurement.humidity),
        );
        publish(
            &mut client_lock,
            &format!("{}/temperature", topic),
            &format!("{:.2}°", measurement.temperature),
        );
        publish(
            &mut client_lock,
            &format!("{}/pressure", topic),
            &format!("{:.2}", measurement.pressure),
        );

        println!(
            "Published: humidity: {} pressure: {} temperature: {}",
            measurement.humidity, measurement.pressure, measurement.temperature
        );

        bme280
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
    num_of_measurements: usize,
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

    while measurements.len() < num_of_measurements {
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
    publish(
        mqtt_client,
        &format!("{}/status", topic),
        &serde_json::to_string_pretty(status).unwrap(),
    );
    publish(
        mqtt_client,
        &format!("{}/pm10", topic),
        &format!("{} ug/m^3", status.pm_10),
    );
    publish(
        mqtt_client,
        &format!("{}/pm1_0", topic),
        &format!("{} ug/m^3", status.pm_1_0),
    );
    publish(
        mqtt_client,
        &format!("{}/pm2_5", topic),
        &format!("{} ug/m^3", status.pm_2_5),
    );

    println!("Published: {:?}", status);
}

fn publish(client: &mut MqttClient, topic: &str, payload: &str) {
    client
        .publish(topic, QoS::AtLeastOnce, true, payload)
        .unwrap();
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
