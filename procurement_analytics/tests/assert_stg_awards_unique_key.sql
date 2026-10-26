select release_id, award_id, count(*) as total
from {{ ref('stg_awards') }}
group by release_id, award_id
HAVING COUNT(*) > 1
