#include<algorithm>
#include<climits>
#include<stdint.h>
#include<string>

#include"Pipes.hh"
#include"TemplateFactory.hh"
#include"StringUtils.hh"

class MaxTemperatureMapper : public HadoopPipes::Mapper {
public:
  MaxTemperatureMapper(HadoopPipes::TaskContext& context){};
  void map(HadoopPipes::MapContext& context){
    std::string line = context.getInputValue();
    std::string year = line.substr(0,4);
    std::string temperature = line.substr(5,5);
    context.emit(year, temperature);
  }
};

class MaxTemperatureReducer : public HadoopPipes::Reducer {
public:
  MaxTemperatureReducer(HadoopPipes::TaskContext& contex){};
  void reduce(HadoopPipes::ReduceContext& context){
    int maxValue = INT_MIN;
    while(context.nextValue()){
      maxValue = std::max(maxValue, HadoopUtils::toInt(context.getInputValue()));
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(maxValue));
  }
};

int main(int argc, char *argv[]){
  return HadoopPipes::runTask(
      HadoopPipes::TemplateFactory<MaxTemperatureMapper, MaxTemperatureReducer>() );
}
