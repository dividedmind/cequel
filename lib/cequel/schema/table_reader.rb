# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # A TableReader will query Cassandra's internal representation of a table's
    # schema, and build a {Table} instance exposing an object representation of
    # that schema
    #
    class TableReader
      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/
      COLLECTION_TYPE_PATTERN =
        /^(list|set|map)<(.+)>$/

      # @return [Table] object representation of the table defined in the
      #   database
      attr_reader :table

      #
      # Read the schema defined in the database for a given table and return a
      # {Table} instance
      #
      # @param (see #initialize)
      # @return (see #read)
      #
      def self.read(keyspace, table_name)
        new(keyspace, table_name).read
      end

      #
      # @param keyspace [Metal::Keyspace] keyspace to read the table from
      # @param table_name [Symbol] name of the table to read
      # @private
      #
      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @table = Table.new(table_name.to_sym)
      end
      private_class_method(:new)

      #
      # Read table schema from the database
      #
      # @return [Table] object representation of table in the database, or
      #   `nil` if no table by given name exists
      #
      # @api private
      #
      def read
        if table_data.present?
          read_properties
          read_columns
          table
        end
      end

      protected

      attr_reader :keyspace, :table_name, :table

      private

      def recognize_storage
        flags = table.property('flags')
        columns = all_columns.sort_by { |c| c['position'] }

        if flags.include? 'compound'
          table.compact_storage = false
        elsif flags.include? 'dense'
          table.compact_storage = true
        else
          table.compact_storage = true
          columns.reject! do |col|
            %w(clustering regular).include? col['kind']
          end
          columns.map! do |col|
            if col['kind'] == 'static'
              col.merge 'kind' => 'regular'
            else
              col
            end
          end
        end

        columns
      end

      def read_columns
        recognize_storage.each do |column|
          case column['kind']
          when 'partition_key'
            table.add_partition_key(
              column['column_name'].to_sym,
              Type.lookup_cql(column['type']),
            )
          when 'clustering'
            table.add_clustering_column(
              column['column_name'].to_sym,
              Type.lookup_cql(column['type']),
              column['clustering_order']
            )
          when 'regular'
            name, type = column.values_at 'column_name', 'type'
            if COLLECTION_TYPE_PATTERN =~ type
              read_collection_column(
                name,
                $1,
                *$2.split(/,\s*/)
              )
            else
              table.add_data_column(
                name.to_sym,
                Type.lookup_cql(type),
                index_of(name)
              )
            end
          else
            fail "unrecognized column kind #{column['kind']}"
          end
        end
      end

      def index_of column
        idx = all_indexes.find do
          |idx| idx['options'] == { 'target' => column }
        end
        idx && idx['index_name'].to_sym
      end

      def read_collection_column(name, collection_type, *internal_types)
        types = internal_types
          .map { |internal| Type.lookup_cql(internal) }
        table.__send__("add_#{collection_type}", name.to_sym, *types)
      end

      def read_properties
        table_data.slice(*Table::STORAGE_PROPERTIES).each do |name, value|
          table.add_property(name, value)
        end
      end

      def parse_composite_types(type_string)
        if COMPOSITE_TYPE_PATTERN =~ type_string
          $1.split(',')
        end
      end

      def table_data
        return @table_data if defined? @table_data
        table_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system_schema.tables
              WHERE keyspace_name = ? AND table_name = ?
        CQL
        @table_data = table_query.first.try(:to_hash)
      end

      def all_columns
        @all_columns ||=
          if table_data
            column_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system_schema.columns
              WHERE keyspace_name = ? AND table_name = ?
            CQL
            column_query.map(&:to_hash)
          end
      end

      def all_indexes
        @all_indexes ||=
          if table_data
            index_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system_schema.indexes
              WHERE keyspace_name = ? AND table_name = ?
            CQL
            index_query.map(&:to_hash)
          end
      end
    end
  end
end
