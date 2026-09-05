//! Regression coverage for the kind:39002 replaceable-event `#p` index.
//!
//! Channel discovery reads the same roster through two different database
//! indexes: `#d` uses `events.d_tag`, while `#p` joins `event_mentions`. This
//! test exercises the relay's production member-snapshot storage path and
//! proves that replacing a roster with a revision containing another `p` tag
//! makes that revision discoverable by the newly added member.

use buzz_core::CommunityId;
use buzz_db::{event::EventQuery, Db};
use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
use sqlx::PgPool;
use uuid::Uuid;

const TEST_DB_URL: &str = "postgres://buzz:buzz_dev@localhost:5432/buzz";

fn admin_url() -> String {
    std::env::var("BUZZ_TEST_DATABASE_URL").unwrap_or_else(|_| TEST_DB_URL.into())
}

async fn create_scratch_db(admin: &PgPool, prefix: &str) -> (PgPool, String) {
    let name = format!("{}_{}", prefix, Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(admin)
        .await
        .expect("create scratch database");

    let base = admin_url();
    let path = base.rfind('/').expect("database URL has a path segment");
    let pool = PgPool::connect(&format!("{}/{name}", &base[..path]))
        .await
        .expect("connect to scratch database");
    buzz_db::migration::run_migrations(&pool)
        .await
        .expect("migrate scratch database");
    (pool, name)
}

async fn drop_scratch_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
    )))
    .execute(admin)
    .await;
}

async fn seed_channel(pool: &PgPool, community: Uuid, channel: Uuid, owner: &Keys) {
    sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
        .bind(community)
        .bind(format!(
            "replaceable-tag-index-{}.example",
            community.simple()
        ))
        .execute(pool)
        .await
        .expect("insert community");

    buzz_db::channel::create_channel_with_id(
        pool,
        CommunityId::from_uuid(community),
        channel,
        &format!("replaceable-tag-index-{channel}"),
        buzz_db::channel::ChannelType::Stream,
        buzz_db::channel::ChannelVisibility::Open,
        None,
        owner.public_key().to_bytes().as_slice(),
        None,
    )
    .await
    .expect("create channel");
}

async fn add_member(
    pool: &PgPool,
    community: Uuid,
    channel: Uuid,
    pubkey: &str,
    invited_by: &[u8],
) {
    sqlx::query(
        "INSERT INTO channel_members \
         (community_id, channel_id, pubkey, role, invited_by) \
         VALUES ($1, $2, $3, 'member'::member_role, $4)",
    )
    .bind(community)
    .bind(channel)
    .bind(hex::decode(pubkey).expect("hex pubkey"))
    .bind(invited_by)
    .execute(pool)
    .await
    .expect("insert channel member");
}

fn roster_event(
    relay_keys: &Keys,
    channel: Uuid,
    created_at: u64,
    members: &[(&str, &str)],
) -> nostr::Event {
    let channel = channel.to_string();
    let mut tags = vec![Tag::parse(["d", channel.as_str()]).expect("d tag")];
    for (pubkey, role) in members {
        tags.push(Tag::parse(["p", pubkey, "", role]).expect("p tag"));
    }
    EventBuilder::new(Kind::Custom(39002), "")
        .tags(tags)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(relay_keys)
        .expect("sign roster")
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn replaceable_roster_ptag_index_tracks_new_revision_pubkeys() {
    let admin = PgPool::connect(&admin_url()).await.expect("connect admin");
    let (pool, scratch_name) = create_scratch_db(&admin, "replaceable_ptag_index").await;
    let db = Db::from_pool(pool.clone());

    let community_uuid = Uuid::new_v4();
    let community = CommunityId::from_uuid(community_uuid);
    let channel = Uuid::new_v4();
    let owner_keys = Keys::generate();
    let relay_keys = Keys::generate();
    seed_channel(&pool, community_uuid, channel, &owner_keys).await;

    let owner = owner_keys.public_key().to_hex();
    let existing_member = Keys::generate().public_key().to_hex();
    let new_member = Keys::generate().public_key().to_hex();
    let invited_by = owner_keys.public_key().to_bytes();
    add_member(
        &pool,
        community_uuid,
        channel,
        &existing_member,
        invited_by.as_slice(),
    )
    .await;

    let created_at = Timestamp::now().as_secs();
    let first = roster_event(
        &relay_keys,
        channel,
        created_at,
        &[
            (owner.as_str(), "owner"),
            (existing_member.as_str(), "member"),
        ],
    );
    let mut snapshot = db
        .lock_member_snapshot(community, channel, &relay_keys.public_key().to_bytes())
        .await
        .expect("lock first member snapshot");
    let (_, inserted) = snapshot
        .replace_member_event(community, channel, &first)
        .await
        .expect("store first roster revision");
    snapshot.release().await.expect("release first snapshot");
    assert!(inserted, "first roster revision must be inserted");

    add_member(
        &pool,
        community_uuid,
        channel,
        &new_member,
        invited_by.as_slice(),
    )
    .await;
    let second = roster_event(
        &relay_keys,
        channel,
        created_at + 1,
        &[
            (owner.as_str(), "owner"),
            (existing_member.as_str(), "member"),
            (new_member.as_str(), "member"),
        ],
    );
    let mut snapshot = db
        .lock_member_snapshot(community, channel, &relay_keys.public_key().to_bytes())
        .await
        .expect("lock replacement member snapshot");
    let (_, inserted) = snapshot
        .replace_member_event(community, channel, &second)
        .await
        .expect("store replacement roster revision");
    snapshot
        .release()
        .await
        .expect("release replacement snapshot");
    assert!(inserted, "replacement roster revision must be inserted");

    let by_d = db
        .query_events(&EventQuery {
            kinds: Some(vec![39002]),
            d_tag: Some(channel.to_string()),
            limit: Some(10),
            ..EventQuery::for_community(community)
        })
        .await
        .expect("query current roster by #d");
    assert_eq!(by_d.len(), 1, "#d must return one live roster");
    assert_eq!(by_d[0].event.id, second.id, "#d must return revision two");
    assert!(by_d[0].event.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.first().map(String::as_str) == Some("p")
            && parts.get(1).map(String::as_str) == Some(new_member.as_str())
    }));

    let by_new_p = db
        .query_events(&EventQuery {
            kinds: Some(vec![39002]),
            p_tag_hex: Some(new_member.clone()),
            limit: Some(10),
            ..EventQuery::for_community(community)
        })
        .await
        .expect("query current roster by newly added #p");
    assert_eq!(by_new_p.len(), 1, "new member must discover one roster");
    assert_eq!(
        by_new_p[0].event.id, second.id,
        "new member's #p query must return revision two"
    );

    // Reproduce the production data shape independently of the now-correct
    // writer: the event and its #d index remain intact while one denormalized
    // mention row is missing.
    sqlx::query(
        "DELETE FROM event_mentions \
         WHERE community_id = $1 AND event_id = $2 AND pubkey_hex = $3",
    )
    .bind(community_uuid)
    .bind(second.id.as_bytes().as_slice())
    .bind(&new_member)
    .execute(&pool)
    .await
    .expect("simulate stale production mention index");
    let stale_by_p = db
        .query_events(&EventQuery {
            kinds: Some(vec![39002]),
            p_tag_hex: Some(new_member.clone()),
            limit: Some(10),
            ..EventQuery::for_community(community)
        })
        .await
        .expect("query stale roster by #p");
    assert!(
        stale_by_p.is_empty(),
        "missing index row must reproduce defect"
    );

    // `buzz-admin reconcile-channels --channel` builds the same canonical
    // roster and calls this replacement method. A later revision restores the
    // complete mention index without changing membership semantics.
    let repaired = roster_event(
        &relay_keys,
        channel,
        created_at + 2,
        &[
            (owner.as_str(), "owner"),
            (existing_member.as_str(), "member"),
            (new_member.as_str(), "member"),
        ],
    );
    let (_, inserted) = db
        .replace_addressable_event(community, &repaired, Some(channel))
        .await
        .expect("force-republish canonical roster");
    assert!(inserted, "repair revision must replace the stale roster");
    let repaired_by_p = db
        .query_events(&EventQuery {
            kinds: Some(vec![39002]),
            p_tag_hex: Some(new_member),
            limit: Some(10),
            ..EventQuery::for_community(community)
        })
        .await
        .expect("query repaired roster by #p");
    assert_eq!(repaired_by_p.len(), 1, "repair must restore #p discovery");
    assert_eq!(
        repaired_by_p[0].event.id, repaired.id,
        "#p must return the force-republished roster"
    );

    drop_scratch_db(&admin, pool, &scratch_name).await;
}
