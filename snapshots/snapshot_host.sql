{% snapshot snapshot_host %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='listing_id',
          check_cols=['HOST_ID', 'HOST_NAME', 'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD'],
        )
    }}

select * from {{ source('raw', 'listings') }}

{% endsnapshot %}