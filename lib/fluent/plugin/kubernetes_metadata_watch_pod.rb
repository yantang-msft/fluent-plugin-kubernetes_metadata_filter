#
# Fluentd Kubernetes Metadata Filter Plugin - Enrich Fluentd events with
# Kubernetes metadata
#
# Copyright 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require_relative 'kubernetes_metadata_common'

module KubernetesMetadata
  module WatchPod

    include ::KubernetesMetadata::Common

    def start_pod_watch
      begin
        return if !metadata_source.pod_name
        watcher = @client.watch_pods({:namespace => @metadata_source.namespace_name, :name => @metadata_source.pod_name})
      rescue Exception => e
        message = "Exception encountered fetching metadata from Kubernetes API endpoint: #{e.message}"
        message += " (#{e.response})" if e.respond_to?(:response)

        raise Fluent::ConfigError, message
      end

      watcher.each do |notice|
        case notice.type
          when 'MODIFIED', 'ADDED'
            update_pod_cache(notice)

            # Update id_cache if pod UID is changed
            pod_id = notice.object['metadata']['uid']
            id_cache_key = get_id_cache_key_given_metadata_source
            id_cached = @id_cache[id_cache_key]
            if id_cached && id_cached[:pod_id] != pod_id
              id_cached[:pod_id] = pod_id
              @stats.bump(:id_cache_watch_updates_pod);
            end
          when 'DELETED'
            # ignore and let age out for cases where pods
            # deleted but still processing logs
            @stats.bump(:pod_cache_watch_delete_ignored)
          else
            # Don't pay attention to creations, since the created pod may not
            # end up on this node.
            @stats.bump(:pod_cache_watch_ignored)
        end
      end
    end
  end
end
