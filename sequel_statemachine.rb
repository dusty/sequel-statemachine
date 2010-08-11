module Sequel
  module Plugins
    module Statemachine
      
      def self.apply(model, options={})
        model.plugin(:schema)
        return false unless model.table_exists?
        attempts = options[:attempts].is_a?(Integer) ? options[:attempts] : 10
        model.const_set('SM_ATTEMPTS', attempts)
      end # self.apply
      
      module InstanceMethods
        def before_create
          initialize_statemachine
          super
        end
        
        def process
          begin
            return false unless self.pending? && before_process
            self.sm_attempts ||= 0
            self.sm_attempts += 1
            self.sm_attempted_at = Time.now
            self.do_process
            complete!
            true
          rescue StandardError => e
            reject!(e)
            false
          end
        end
        
        def pending?
          self.sm_state == 'pending'
        end
        
        def failed?
          self.sm_state == 'failed'
        end
        
        def completed?
          self.sm_state == 'completed'
        end
        
        def fail(msg)
          self.sm_error = msg
          self.sm_state = 'failed'
        end
        
        def fail!(msg)
          self.fail(msg)
          self.save
        end
        
        def reject(msg)
          self.sm_error = msg
          self.check_for_failure
        end
        
        def reject!(msg)
          reject(msg)
          self.save
        end
        
        def complete
          self.sm_state = 'completed'
          self.sm_completed_at = Time.now
        end
        
        def complete!
          self.complete
          self.save
        end
        
        def retry
          self.sm_state = 'pending'
          self.process
        end
        
        def sm_error=(error)
          if error.is_a?(StandardError)
            self[:sm_error] = "#{error.class} : #{error.message}"
          else
            self[:sm_error] = error
          end
        end
        
        protected
        def do_process
          raise StandardError.new("Override this in class")
        end
        
        def check_for_failure
          if self.sm_attempts >= self.class::SM_ATTEMPTS
            self.sm_state = 'failed' 
          end
        end
        
        def before_process
          true
        end
        
        def initialize_statemachine
          self.sm_state = 'pending' if self.sm_state.blank?
          self.sm_attempts = 0
        end

      end # InstanceMethods
      
      module DatasetMethods
        def pending
          filter(:sm_state => 'pending').
          order(:sm_attempts.asc, :sm_attempted_at.asc)
        end
        def completed
          filter(:sm_state => 'completed')
        end
        def failed
          filter(:sm_state => 'failed')
        end
      end # Dataset Methods
      
    end # Statemachine
  end # Plugins
end # Sequel