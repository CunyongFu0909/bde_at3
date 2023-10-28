{% snapshot snapshot_room %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='listing_id',
          check_cols=['ROOM_TYPE', 'ACCOMMODATES'],
        )
    }}

select * from {{ source('raw', 'listings') }}

{% endsnapshot %}