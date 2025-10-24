from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo
from typing import Dict, List, Optional, Literal, Any


class SourceConfig(BaseModel):
    reader_type: Literal[
        "file_stream", "api", "api_by_id", "table_stream", "table_batch", "dlt_stream"
    ]
    host_name_secret_key: Optional[str] = None
    path: Optional[str] = None
    username_secret_key: Optional[str] = None
    password_secret_key: Optional[str] = None
    id_key: Optional[str] = None
    read_options: Dict = Field(default_factory=dict)
    format: Optional[str] = None
    volume_path: Optional[str] = None
    explode_column: Optional[str] = None
    filter_condition: Optional[str] = None
    table_name: Optional[str] = None
    source_watermark_column: str = "md_updated_datetime"
    batch_window: int = 0
    keep_source_metadata: bool = True
    generated_metadata_columns_to_remove: List[str] = []
    source_metadata_columns_to_keep: List[str] = []
    watermark_table_name: Optional[str] = None
    watermark_table_datetime_column: str = "md_created_datetime"
    enable_file_watermarking: Optional[bool] = None
    select_query: Optional[str] = None
    load_type: Optional[str] = None
    schema_location: Optional[str] = None
    select_columns: List[str] = []

    @field_validator("table_name", "watermark_table_name")
    def validate_table_name(cls, v):
        if v is not None and len(v.split(".")) < 2:
            raise ValueError("Full table name with schema is required")
        return v

    @model_validator(mode="before")
    def check_format_for_file_stream(cls, values):
        reader_type = values.get("reader_type")
        fmt = values.get("format")
        if reader_type == "file_stream" and not fmt:
            raise ValueError(
                "format must be provided when reader_type is 'file_stream'"
            )
        return values

    @model_validator(mode="before")
    def check_table_name_for_table_batch(cls, values):
        reader_type = values.get("reader_type")
        table_name = values.get("table_name")
        sql_query = values.get("select_query")
        if reader_type == "table_batch" and not sql_query and not table_name:
            raise ValueError(
                "table_name or select_query is required when reader_type is 'table_batch'"
            )
        return values

    @model_validator(mode="before")
    def check_table_name_for_table_stream(cls, values):
        reader_type = values.get("reader_type")
        table_name = values.get("table_name")
        if reader_type == "table_stream" and not table_name:
            raise ValueError(
                "table_name is required when reader_type is 'table_stream'"
            )
        return values


class TargetConfig(BaseModel):
    writer_type: Literal[
        "append_stream", 
        "batch", 
        "merge_batch",
        "dlt_append_flow",
        "dlt_auto_cdc",
        "dlt_auto_cdc_snapshot", 
        "dlt_sink",
        "dlt_table",
        "dlt_view"
    ]
    table_name: Optional[str] = None
    view_name: Optional[str] = None
    trigger_type_value: Dict = Field(default_factory=lambda: {"availableNow": True})
    writer_mode: Optional[Literal["append", "overwrite"]] = None
    write_options: Dict = Field(default_factory=dict)
    delete_condition: Optional[str] = None
    enable_file_watermarking: bool = False
    meta_table_name: Optional[str] = None
    join_columns: Optional[List[str]] = None
    custom_update_condition: Optional[str] = None
    update_only: bool = False
    exclude_columns: Optional[List[str]] = None
    change_status_column: str = "md_change_status"
    updated_datetime_column: str = "md_updated_datetime"
    
    # DLT-specific configurations
    materialized_view: bool = False
    expectations: Optional[List[Dict[str, Any]]] = None
    partition_cols: Optional[List[str]] = None
    table_properties: Dict[str, str] = Field(default_factory=dict)
    comment: Optional[str] = None
    path: Optional[str] = None
    schema_evolution_mode: Optional[Literal["addNewColumns", "rescue"]] = None
    sequence_by: Optional[str] = None
    scd_type: Optional[Literal["1", "2"]] = None
    track_history_column_list: Optional[List[str]] = None
    ignore_null_updates: bool = False
    
    # DLT append_flow specific parameters
    flow_name: Optional[str] = None
    once: bool = False
    spark_conf: Dict[str, str] = Field(default_factory=dict)
    
    # DLT create_auto_cdc_flow specific parameters
    keys: Optional[List[str]] = None
    apply_as_deletes: Optional[str] = None
    apply_as_truncates: Optional[str] = None
    column_list: Optional[List[str]] = None
    except_column_list: Optional[List[str]] = None
    stored_as_scd_type: Optional[Literal["1", "2"]] = None
    track_history_except_column_list: Optional[List[str]] = None
    
    # DLT create_sink specific parameters
    sink_name: Optional[str] = None
    format: Optional[str] = None
    options: Dict[str, str] = Field(default_factory=dict)
    
    # DLT create_streaming_table specific parameters
    cluster_by_auto: bool = True
    cluster_by: Optional[List[str]] = None
    schema: Optional[str] = None
    expect_all: Dict[str, str] = Field(default_factory=dict)
    expect_all_or_drop: Dict[str, str] = Field(default_factory=dict)
    expect_all_or_fail: Dict[str, str] = Field(default_factory=dict)
    row_filter: Optional[str] = None
    temporary: bool = False

    @field_validator("table_name")
    def validate_target_table_name(cls, v, info: ValidationInfo):
        writer_type = info.data.get("writer_type")
        # table_name is required for all writer types except dlt_view
        if writer_type != "dlt_view" and (not v or len(v.split(".")) < 2):
            raise ValueError("target_table_name with its schema name is required")
        # If table_name is provided, validate format
        if v and len(v.split(".")) < 2:
            raise ValueError("target_table_name with its schema name is required")
        return v

    @field_validator("view_name")
    def validate_view_name(cls, v, info: ValidationInfo):
        writer_type = info.data.get("writer_type")
        # view_name is required for dlt_view writer type - check for empty string
        if writer_type == "dlt_view" and v == "":
            raise ValueError("view_name is required for dlt_view writer")
        # view_name can be either simple name or schema-qualified name for DLT views
        # DLT views are created within the pipeline's schema context
        return v

    @model_validator(mode="before")
    def check_view_name_for_dlt_view(cls, values):
        writer_type = values.get("writer_type")
        view_name = values.get("view_name")
        if writer_type == "dlt_view" and not view_name:
            raise ValueError("view_name is required for dlt_view writer")
        return values

    @field_validator("meta_table_name")
    def validate_meta_table_name(cls, v, info: ValidationInfo):
        if info.data.get("enable_file_watermarking") and (
            not v or len(v.split(".")) < 2
        ):
            raise ValueError(
                "meta_table_name with its schema name is required when enable_file_watermarking is True"
            )
        return v

    @field_validator("join_columns")
    def validate_join_columns(cls, v, info: ValidationInfo):
        if info.data.get("writer_type") == "merge_batch" and (not v or len(v) == 0):
            raise ValueError(
                "join_columns with list of column names to join on is required in entity_config"
            )
        return v

    @model_validator(mode="before")
    def check_writer_mode_for_batch(cls, values):
        writer_type = values.get("writer_type")
        writer_mode = values.get("writer_mode")
        if writer_type == "batch" and not writer_mode:
            raise ValueError("writer_mode must be provided when writer_type is 'batch'")
        return values


class TransformConfig(BaseModel):
    transform_type: Literal["default", "json"] = "default"
    mapping_config: Optional[str] = None

    @model_validator(mode="before")
    def check_mapping_config_for_json_transformer(cls, values):
        transform_type = values.get("transform_type")
        mapping_config = values.get("mapping_config")
        if transform_type == "json" and not mapping_config:
            raise ValueError(
                "mapping_config must be provided when transform_type is 'json'"
            )
        return values


class EntityConfig(BaseModel):
    entity_name: Optional[str] = None
    source: Optional[SourceConfig] = None
    target: Optional[TargetConfig] = None
    transform: Optional[TransformConfig] = None

    @model_validator(mode="before")
    def set_default_transform(cls, values):
        if "transform" not in values:
            values["transform"] = TransformConfig()
        return values


class RuleConfig(BaseModel):
    rule_name: Optional[str] = None
    validator: str
    source_table_name: str
    results_table_name: str
    unique_key: str
    select_query: Optional[str] = None
    subset_method: str
    subset_column_list: Optional[List[str]] = None
    validation_status: Optional[str] = None
    validation_status_flag: bool = False
    query_params: Optional[Dict[str, Any]] = None

    @field_validator("source_table_name", "results_table_name")
    def validate_table_name(cls, v):
        if len(v.split(".")) < 2:
            raise ValueError("Full table name with schema is required")
        return v

    @field_validator("subset_column_list")
    def validate_subset_column_list(cls, v, info: ValidationInfo):
        if info.data.get("subset_method") == "custom_columns_source_data" and not v:
            raise ValueError(
                "subset_column_list must be provided for custom_columns_source_data subset_method"
            )
        return v
