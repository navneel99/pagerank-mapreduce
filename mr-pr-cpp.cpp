// #include <boost/config.hpp>
// #if defined(BOOST_MSVC)
// #   pragma warning(disable: 4127)

// // turn off checked iterators to avoid performance hit
// #   if !defined(__SGI_STL_PORT)  &&  !defined(_DEBUG)
// #       define _SECURE_SCL 0
// #       define _HAS_ITERATOR_DEBUGGING 0
// #   endif
// #endif

#include "mapreduce.hpp"
#include <iostream>

// namespace mapreduce {

// template<typename MapTask,
// 		 typename ReduceTask,
// 		 typename Datasource=datasource::directory_iterator<MapTask>,
// 		 typename Combiner=null_combiner,
// 		 typename IntermediateStore=intermediates::local_disk<MapTask> >
// class job;

// } // namespace mapreduce

// namespace mapreduce {

template<typename MapTask>
class map_task
{
    public:
        typedef std::string   key_type;
        typedef std::ifstream value_type;
        typedef std::string   intermediate_key_type;
        typedef unsigned      intermediate_value_type;

        map_task(job::map_task_runner &runner);
        void operator()(key_type const &key, value_type const &value);

};

class reduce_task
{
  public:
	typedef std::string  key_type;
	typedef size_t       value_type;

	reduce_task(job::reduce_task_runner &runner);

	template<typename It>
	void operator()(typename map_task::intermediate_key_type const &key, It it, It ite)
};

// };

// typedef mapreduce::job


int main(){
    std::cout<<"Test\n";
    return 0;
}