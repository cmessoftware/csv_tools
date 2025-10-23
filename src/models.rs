use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MorososTransmitDynamoDbModel {
    pub Cuil: f64,
    pub NroDoc: String,
    pub ApellidoNombre: String,
    pub IdCliente: i32,
    pub IdRegion: i32,
    pub RazonSocial: String,
    pub Telefono: String,
    pub NombreRegion: String,
    pub NombreCategoria: String,
    pub Periodo: String,
    pub IdEntidad: i32,
    pub CreateDate: String,
    pub CreateUser: String,
}