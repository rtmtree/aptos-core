// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::Error,
    poller::{poll_peer, DataSummaryPoller},
    tests::mock::MockNetwork,
};
use aptos_config::{
    config::{AptosDataClientConfig, AptosDataPollerConfig},
    network_id::PeerNetworkId,
};
use aptos_storage_service_types::StorageServiceError;
use claims::assert_matches;
use maplit::hashset;
use std::{collections::HashSet, thread::sleep};

#[tokio::test]
async fn identify_peers_to_poll_rounds() {
    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, None, None);

    // Add several priority peers
    let num_priority_peers = 10;
    let mut priority_peers = hashset![];
    for _ in 0..num_priority_peers {
        priority_peers.insert(mock_network.add_peer(true));
    }

    // Add several regular peers
    let num_regular_peers = 20;
    let mut regular_peers = hashset![];
    for _ in 0..num_regular_peers {
        regular_peers.insert(mock_network.add_peer(false));
    }

    // Fetch the priority peers to poll multiple times and verify no regular peers are returned
    let num_polling_rounds = 100;
    for _ in 0..num_polling_rounds {
        let peers_to_poll = poller.identify_peers_to_poll(true).unwrap();
        for peer in peers_to_poll {
            assert!(priority_peers.contains(&peer));
            assert!(!regular_peers.contains(&peer));
        }
    }

    // Fetch the regular peers to poll multiple times and verify no priority peers are returned
    for _ in 0..num_polling_rounds {
        let peers_to_poll = poller.identify_peers_to_poll(false).unwrap();
        for peer in peers_to_poll {
            assert!(!priority_peers.contains(&peer));
            assert!(regular_peers.contains(&peer));
        }
    }

    // Fetch the peers to poll in alternating loops and verify
    // that we receive the expected peers.
    for i in 0..num_polling_rounds {
        // Alternate between polling priority and regular peers
        let poll_priority_peers = i % 2 == 0;

        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        for peer in peers_to_poll {
            if poll_priority_peers {
                // Verify the peer is a priority peer
                assert!(priority_peers.contains(&peer));
                assert!(!regular_peers.contains(&peer));
            } else {
                // Verify the peer is a regular peer
                assert!(!priority_peers.contains(&peer));
                assert!(regular_peers.contains(&peer));
            }
        }
    }
}

#[tokio::test]
async fn identify_peers_to_poll_frequencies() {
    // Create the data client config
    let data_client_config = AptosDataClientConfig {
        data_poller_config: AptosDataPollerConfig {
            additional_polls_per_peer_bucket: 1,
            min_polls_per_second: 5,
            max_polls_per_second: 20,
            peer_bucket_size: 10,
            poll_loop_interval_ms: 100,
            ..Default::default()
        },
        ..Default::default()
    };

    // Test priority and regular peers
    for poll_priority_peers in [false, true] {
        // Create a list of peer counts and expected peer polling frequencies.
        // Format is: (peer_count, expected_polls_per_second).
        let peer_counts_and_polls_per_second = vec![
            (1, 5.0),
            (9, 5.0),
            (10, 6.0),
            (19, 6.0),
            (25, 7.0),
            (39, 8.0),
            (40, 9.0),
            (51, 10.0),
            (69, 11.0),
            (79, 12.0),
            (80, 13.0),
            (99, 14.0),
            (100, 15.0),
            (110, 16.0),
            (121, 17.0),
            (139, 18.0),
            (149, 19.0),
            (150, 20.0),
            (160, 20.0),
            (200, 20.0),
        ];

        // Test various peer counts and expected peer polling frequencies
        for (peer_count, expected_polls_per_second) in peer_counts_and_polls_per_second {
            // Create a mock network with a poller
            let (mut mock_network, _, _, poller) =
                MockNetwork::new(None, Some(data_client_config), None);

            // Add the expected number of peers
            let mut peers = vec![];
            for _ in 0..peer_count {
                peers.push(mock_network.add_peer(poll_priority_peers));
            }

            // Sum the peers to poll over many rounds
            let num_polling_rounds = 2_000;
            let mut total_num_polls = 0;
            for _ in 0..num_polling_rounds {
                let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
                total_num_polls += peers_to_poll.len();
            }

            // Calculate the number of polls per second
            let num_polling_rounds_per_second = 1_000.0
                / (2.0 * data_client_config.data_poller_config.poll_loop_interval_ms as f64);
            let num_polling_seconds = (num_polling_rounds as f64) / num_polling_rounds_per_second;
            let num_polls_per_second = (total_num_polls as f64) / num_polling_seconds;

            // Verify the number of polls per second is within a reasonable delta
            assert!((num_polls_per_second - expected_polls_per_second).abs() < 1.0);
        }
    }
}

#[tokio::test]
async fn identify_peers_to_poll_disconnected() {
    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, None, None);

    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );

        // Add peer 1
        let peer_1 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 1
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);

        // Add peer 2 and disconnect peer 1
        let peer_2 = mock_network.add_peer(poll_priority_peers);
        mock_network.disconnect_peer(peer_1);

        // Request the next set of peers to poll and verify it's peer 2
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_2]);

        // Disconnect peer 2
        mock_network.disconnect_peer(peer_2);

        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );

        // Add peer 3
        let peer_3 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 3
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_3]);

        // Disconnect peer 3
        mock_network.disconnect_peer(peer_3);

        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );
    }
}

#[tokio::test]
async fn identify_peers_to_poll_ordering() {
    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, None, None);

    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Add peer 1
        let peer_1 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 1
        for _ in 0..3 {
            let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
            assert_eq!(peers_to_poll, hashset![peer_1]);
            poller.in_flight_request_started(poll_priority_peers, &peer_1);
            poller.in_flight_request_complete(&peer_1);
        }

        // Add peer 2
        let peer_2 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's either peer
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert!(peers_to_poll == hashset![peer_1] || peers_to_poll == hashset![peer_2]);
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll),
        );
        poller.in_flight_request_complete(&get_single_peer_from_set(&peers_to_poll));

        // Request the next set of peers to poll and don't mark the request as complete
        let peers_to_poll_1 = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll_1),
        );

        // Request another set of peers to poll and verify it's the other peer
        let peers_to_poll_2 = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll_2),
        );
        assert_ne!(peers_to_poll_1, peers_to_poll_2);

        // Neither poll has completed (they're both in-flight), so make another request
        // and verify we get no peers.
        assert!(poller
            .identify_peers_to_poll(poll_priority_peers)
            .unwrap()
            .is_empty());

        // Add peer 3
        let peer_3 = mock_network.add_peer(poll_priority_peers);

        // Request another peer again and verify it's peer_3
        let peers_to_poll_3 = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll_3, hashset![peer_3]);
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll_3),
        );

        // Mark the second poll as completed
        poller.in_flight_request_complete(&get_single_peer_from_set(&peers_to_poll_2));

        // Make another request and verify we get peer 2 now (as it was ready)
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, peers_to_poll_2);
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll_2),
        );

        // Mark the first poll as completed
        poller.in_flight_request_complete(&get_single_peer_from_set(&peers_to_poll_1));

        // Make another request and verify we get peer 1 now
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, peers_to_poll_1);
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll_1),
        );

        // Mark the third poll as completed
        poller.in_flight_request_complete(&get_single_peer_from_set(&peers_to_poll_3));

        // Make another request and verify we get peer 3 now
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, peers_to_poll_3);
        poller.in_flight_request_complete(&get_single_peer_from_set(&peers_to_poll_3));
    }
}

#[tokio::test]
async fn identify_peers_to_poll_reconnected() {
    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, None, None);

    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );

        // Add peer 1
        let peer_1 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 1
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);

        // Add peer 2 and disconnect peer 1
        let peer_2 = mock_network.add_peer(poll_priority_peers);
        mock_network.disconnect_peer(peer_1);

        // Request the next set of peers to poll and verify it's peer 2
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_2]);

        // Disconnect peer 2 and reconnect peer 1
        mock_network.disconnect_peer(peer_2);
        mock_network.reconnect_peer(peer_1);

        // Request the next set of peers to poll and verify it's peer 1
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);

        // Disconnect peer 1
        mock_network.disconnect_peer(peer_1);

        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );
    }
}

#[tokio::test]
async fn identify_peers_to_poll_reconnected_in_flight() {
    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, None, None);

    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );

        // Add peer 1
        let peer_1 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 1.
        // Mark the request as in-flight but not completed.
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);
        poller.in_flight_request_started(poll_priority_peers, &peer_1);

        // Add peer 2 and disconnect peer 1
        let peer_2 = mock_network.add_peer(poll_priority_peers);
        mock_network.disconnect_peer(peer_1);

        // Request the next set of peers to poll and verify it's peer 2.
        // Mark the request as in-flight but not completed.
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_2]);
        poller.in_flight_request_started(poll_priority_peers, &peer_2);

        // Request the next set of peers to poll and verify no peers are returned
        // (peer 2's request is still in-flight).
        for _ in 0..10 {
            assert_eq!(
                poller.identify_peers_to_poll(poll_priority_peers),
                Ok(hashset![])
            );
        }

        // Reconnect peer 1
        poller.in_flight_request_complete(&peer_1);
        mock_network.reconnect_peer(peer_1);

        // Request the next set of peers to poll and verify it's peer 1
        // (peer 2's request is still in-flight).
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);
        poller.in_flight_request_started(poll_priority_peers, &peer_1);

        // Mark peer 2's request as complete
        poller.in_flight_request_complete(&peer_2);

        // Request the next set of peers to poll and verify it's peer 2
        // (peer 1's request is still in-flight).
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_2]);

        // Disconnect peer 2
        mock_network.disconnect_peer(peer_2);

        // Request the next set of peers to poll and verify no peers are returned
        // (peer s's request is still in-flight).
        assert_eq!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Ok(hashset![])
        );

        // Mark peer 1's request as complete
        poller.in_flight_request_complete(&peer_1);

        // Request the next set of peers to poll multiple times and
        // verify peer 1 is returned each time.
        for _ in 0..10 {
            let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
            assert_eq!(peers_to_poll, hashset![peer_1]);
            poller.in_flight_request_started(poll_priority_peers, &peer_1);
            poller.in_flight_request_complete(&peer_1);
        }

        // Disconnect peer 1
        mock_network.disconnect_peer(peer_1);

        // Request the next set of peers to poll and verify we have no peers
        assert_matches!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Err(Error::DataIsUnavailable(_))
        );
    }
}

#[tokio::test]
async fn identify_peers_to_poll_max_in_flight() {
    // Create a data client with max in-flight requests of 2
    let data_client_config = AptosDataClientConfig {
        data_poller_config: AptosDataPollerConfig {
            max_num_in_flight_priority_polls: 2,
            max_num_in_flight_regular_polls: 2,
            ..Default::default()
        },
        ..Default::default()
    };

    // Create a mock network with a poller
    let (mut mock_network, _, _, poller) = MockNetwork::new(None, Some(data_client_config), None);

    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Add peer 1
        let peer_1 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 1.
        // Mark the request as in-flight but not completed.
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_1]);
        poller.in_flight_request_started(poll_priority_peers, &peer_1);

        // Add peer 2
        let peer_2 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify it's peer 2
        // (peer 1's request has not yet completed). Mark the request as
        // in-flight but not completed.
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert_eq!(peers_to_poll, hashset![peer_2]);
        poller.in_flight_request_started(poll_priority_peers, &peer_2);

        // Add peer 3
        let peer_3 = mock_network.add_peer(poll_priority_peers);

        // Request the next set of peers to poll and verify none are returned
        // (we already have the maximum number of in-flight requests).
        assert_eq!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Ok(hashset![])
        );

        // Mark peer 2's in-flight request as complete
        poller.in_flight_request_complete(&peer_2);

        // Request the next set of peers to poll and verify it's either peer 2 or peer 3
        let peers_to_poll = poller.identify_peers_to_poll(poll_priority_peers).unwrap();
        assert!(peers_to_poll == hashset![peer_2] || peers_to_poll == hashset![peer_3]);
        poller.in_flight_request_started(
            poll_priority_peers,
            &get_single_peer_from_set(&peers_to_poll),
        );

        // Request the next set of peers to poll and verify none are returned
        // (we already have the maximum number of in-flight requests).
        assert_eq!(
            poller.identify_peers_to_poll(poll_priority_peers),
            Ok(hashset![])
        );

        // Mark peer 1's in-flight request as complete
        poller.in_flight_request_complete(&peer_1);

        // Request the next set of peers to poll and verify it's not the
        // peer that already has an in-flight request.
        assert_ne!(
            poller.identify_peers_to_poll(poll_priority_peers).unwrap(),
            peers_to_poll
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn poll_peers_error_handling() {
    // Create a data client with max in-flight requests of 1
    let data_client_config = AptosDataClientConfig {
        data_poller_config: AptosDataPollerConfig {
            max_num_in_flight_priority_polls: 1,
            max_num_in_flight_regular_polls: 1,
            ..Default::default()
        },
        ..Default::default()
    };

    // Test both invalid and dropped responses
    for invalid_response in [true, false] {
        // Create a mock network with a poller
        let (mut mock_network, _, _, poller) =
            MockNetwork::new(None, Some(data_client_config), None);

        // Verify we have no in-flight polls
        let num_in_flight_polls = get_num_in_flight_polls(poller.clone());
        assert_eq!(num_in_flight_polls, 0);

        // Add a peer
        let is_priority_peer = true;
        let peer = mock_network.add_peer(is_priority_peer);

        // Poll the peer
        let handle = poll_peer(poller.clone(), is_priority_peer, peer);

        // Handle the poll request
        sleep(std::time::Duration::from_millis(5_000));
        if let Some(network_request) = mock_network.next_request().await {
            if invalid_response {
                // Send an invalid response
                network_request
                    .response_sender
                    .send(Err(StorageServiceError::InternalError(
                        "An unexpected error occurred!".into(),
                    )));
            } else {
                // Drop the network request
                drop(network_request)
            }
        }

        // Wait for the poller to complete
        handle.await.unwrap();

        // Verify we have no in-flight polls
        let num_in_flight_polls = get_num_in_flight_polls(poller.clone());
        assert_eq!(num_in_flight_polls, 0);
    }
}

/// Fetches the number of in-flight polling requests for peers
fn get_num_in_flight_polls(poller: DataSummaryPoller) -> u64 {
    poller.all_peers_with_in_flight_polls().len() as u64
}

/// Returns the single peer from the given set
fn get_single_peer_from_set(single_peer_set: &HashSet<PeerNetworkId>) -> PeerNetworkId {
    // Verify the set only contains a single peer
    assert_eq!(single_peer_set.len(), 1);

    // Get the single peer from the set
    *single_peer_set.iter().next().unwrap()
}
