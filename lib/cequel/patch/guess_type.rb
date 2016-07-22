module Cequel
  module Patch
    module GuessType
      # the built in guess_type needs some help sometimes.
      def guess_type(object)
        case object
        when ::Hash
          return Cassandra::Types.map(
            Cassandra::Types.int, Cassandra::Types.int
          ) if object.empty?
       end
        super
      end
    end
  end
end

require 'cassandra/util'

Cassandra::Util.singleton_class.prepend Cequel::Patch::GuessType
