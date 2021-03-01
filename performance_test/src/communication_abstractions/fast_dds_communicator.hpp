// Copyright 2021 Apex.AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef COMMUNICATION_ABSTRACTIONS__FAST_DDS_COMMUNICATOR_HPP_
#define COMMUNICATION_ABSTRACTIONS__FAST_DDS_COMMUNICATOR_HPP_

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/topic/TopicDescription.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/PublisherQos.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

#include <atomic>

#include "communicator.hpp"
#include "resource_manager.hpp"
#include "../experiment_configuration/topics.hpp"
#include "../experiment_configuration/qos_abstraction.hpp"

namespace performance_test
{

/// Translates abstract QOS settings to specific QOS settings for FastDDS.
class FastDDSQOSAdapter
{
public:
  /**
   * \brief Constructs the QOS adapter.
   * \param qos The abstract QOS settings the adapter should use to derive the implementation specific QOS settings.
   */
  explicit FastDDSQOSAdapter(const QOSAbstraction qos)
  : m_qos(qos)
  {}

  /// Returns derived FastDDS reliability setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::ReliabilityQosPolicyKind reliability_kind() const
  {
    if (m_qos.reliability == QOSAbstraction::Reliability::BEST_EFFORT) {
      return eprosima::fastdds::dds::ReliabilityQosPolicyKind::BEST_EFFORT_RELIABILITY_QOS;
    } else if (m_qos.reliability == QOSAbstraction::Reliability::RELIABLE) {
      return eprosima::fastdds::dds::ReliabilityQosPolicyKind::RELIABLE_RELIABILITY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastDDS durability setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::DurabilityQosPolicyKind durability_kind() const
  {
    if (m_qos.durability == QOSAbstraction::Durability::VOLATILE) {
      return eprosima::fastdds::dds::DurabilityQosPolicyKind::VOLATILE_DURABILITY_QOS;
    } else if (m_qos.durability == QOSAbstraction::Durability::TRANSIENT_LOCAL) {
      return eprosima::fastdds::dds::DurabilityQosPolicyKind::TRANSIENT_LOCAL_DURABILITY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastDDS history policy setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::HistoryQosPolicyKind history_kind() const
  {
    if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_ALL) {
      return eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_ALL_HISTORY_QOS;
    } else if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_LAST) {
      return eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_LAST_HISTORY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastDDS history depth setting from the stored abstract QOS setting.
  int32_t history_depth() const
  {
    if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_LAST) {
      return static_cast<int32_t>(m_qos.history_depth);
    } else if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_ALL) {
      // Keep all, keeps all. No depth required, but setting to dummy value.
      return 1;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns the number of samples to be allocated on the history
  int32_t resource_limits_samples() const
  {
    return static_cast<int32_t>(m_qos.history_depth);
  }
  /// Returns the publish mode policy from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::PublishModeQosPolicyKind publish_mode() const
  {
    if (m_qos.sync_pubsub) {
      return eprosima::fastdds::dds::PublishModeQosPolicyKind::SYNCHRONOUS_PUBLISH_MODE;
    } else {
      return eprosima::fastdds::dds::PublishModeQosPolicyKind::ASYNCHRONOUS_PUBLISH_MODE;
    }
  }

private:
  const QOSAbstraction m_qos;
};

/**
 * \brief Communication plugin for FastDDS.
 * \tparam Topic The topic type to use.
 *
 * The code in there is derived from
 * https://github.com/eProsima/Fast-DDS/tree/master/examples/C%2B%2B/HelloWorldExample.
 */
template<class Topic>
class FastDDSCommunicator : public Communicator
{
public:
  /// The topic type to use.
  using TopicType = typename Topic::EprosimaTopicType;
  /// The data type to publish and subscribe to.
  using DataType = typename Topic::EprosimaType;

  /// Constructor which takes a reference \param lock to the lock to use.
  explicit FastDDSCommunicator(SpinLock & lock)
  : Communicator(lock),
    m_publisher(nullptr),
    m_subscriber(nullptr),
    m_datawriter(nullptr),
    m_datareader(nullptr),
    m_topic_type(new TopicType())
  {
    m_participant = ResourceManager::get().fastdds_participant();
    if (m_ec.use_single_participant()) {
      m_topic_type.register_type(m_participant);
    } else {
      m_topic_type.register_type(m_participant);
    }
  }

  /**
   * \brief Publishes the provided data.
   *
   *  The first time this function is called it also creates the data writer.
   *  Further it updates all internal counters while running.
   * \param data The data to publish.
   * \param time The time to fill into the data field.
   */
  void publish(DataType & data, const std::chrono::nanoseconds time)
  {
    if (!m_topic)
    {
      create_topic();
    }

    if (!m_datawriter) {
      const FastDDSQOSAdapter qos(m_ec.qos());

      eprosima::fastdds::dds::DataWriterQos dw_qos;

      dw_qos.reliability().kind = qos.reliability_kind();
      dw_qos.durability().kind = qos.durability_kind();
      dw_qos.publish_mode().kind = qos.publish_mode();
      dw_qos.reliable_writer_qos().times.heartbeatPeriod.seconds = 2; // TODO(eprosima) check: hardcoded in old version
      dw_qos.reliable_writer_qos().times.heartbeatPeriod.fraction(
        200 * 1000 * 1000); // TODO(eprosima) check: hardcoded in old version

      m_datawriter = m_publisher->create_datawriter(m_topic, dw_qos);
    }
    lock();
    data.time_(time.count());
    data.id_(next_sample_id());
    increment_sent();  // We increment before publishing so we don't have to lock twice.
    unlock();
    m_datawriter->write(static_cast<void *>(&data));
  }
  /**
   * \brief Reads received data from DDS.
   *
   * In detail this function:
   * * Reads samples from DDS.
   * * Verifies that the data arrived in the right order, chronologically and also consistent with the publishing order.
   * * Counts received and lost samples.
   * * Calculates the latency of the samples received and updates the statistics accordingly.
   */
  void update_subscription()
  {
    if (!m_topic)
    {
      create_topic();
    }

    if (!m_datareader) {
      const FastDDSQOSAdapter qos(m_ec.qos());

      eprosima::fastdds::dds::DataReaderQos dr_qos;

      dr_qos.reliability().kind = qos.reliability_kind();
      dr_qos.durability().kind = qos.durability_kind();
      dr_qos.history().kind = qos.history_kind();
      dr_qos.history().depth = qos.history_depth();
      dr_qos.resource_limits().max_samples = qos.resource_limits_samples();
      dr_qos.resource_limits().allocated_samples = qos.resource_limits_samples();

      m_datareader = m_subscriber->create_datareader(m_topic, dr_qos);
    }

    m_datareader->wait_for_unread_message(eprosima::fastrtps::Duration_t(120)); // max 2 minutes by default
    lock();

    while (m_datareader->take_next_sample(&m_data, &m_info) == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK) {
      if (m_info.instance_state == eprosima::fastdds::dds::ALIVE_INSTANCE_STATE) {
        if (m_prev_timestamp >= m_data.time_()) {
          throw std::runtime_error(
                  "Data consistency violated. Received sample with not strictly "
                  "older timestamp. Time diff: " + std::to_string(
                    m_data.time_() - m_prev_timestamp) + " Data Time: " +
                  std::to_string(m_data.time_())
          );
        }


        if (m_ec.roundtrip_mode() == ExperimentConfiguration::RoundTripMode::RELAY) {
          unlock();
          publish(m_data, std::chrono::nanoseconds(m_data.time_()));
          lock();
        } else {
          m_prev_timestamp = m_data.time_();
          update_lost_samples_counter(m_data.id_());
          add_latency_to_statistics(m_data.time_());
          increment_received();
        }
      }
    }

    unlock();
  }

  /// Returns the data received in bytes.
  std::size_t data_received()
  {
    return num_received_samples() * sizeof(DataType);
  }

private:
  eprosima::fastdds::dds::DomainParticipant * m_participant;
  eprosima::fastdds::dds::Publisher * m_publisher;
  eprosima::fastdds::dds::Subscriber * m_subscriber;
  eprosima::fastdds::dds::DataWriter * m_datawriter;
  eprosima::fastdds::dds::DataReader * m_datareader;

  eprosima::fastdds::dds::Topic* m_topic;

  eprosima::fastdds::dds::SampleInfo m_info;

  eprosima::fastdds::dds::TypeSupport m_topic_type;
  DataType m_data;

  static bool s_type_registered;

  void create_topic()
  {
    m_topic = m_participant->create_topic(m_topic_type->getName(), Topic::topic_name() + m_ec.pub_topic_postfix(), eprosima::fastdds::dds::TOPIC_QOS_DEFAULT);
  }
};

template<class Topic>
bool FastDDSCommunicator<Topic>::s_type_registered = false;

}  // namespace performance_test

#endif  // COMMUNICATION_ABSTRACTIONS__FAST_DDS_COMMUNICATOR_HPP_
