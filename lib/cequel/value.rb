module Cequel
  # A ruby value with associated Cassandra type.
  class Value < SimpleDelegator
    def initialize value, type
      super value
      @type = type
    end

    attr_accessor :type
  end
end
