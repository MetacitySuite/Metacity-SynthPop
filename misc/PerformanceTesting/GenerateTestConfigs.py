import xml.etree.ElementTree as ET
from argparse import ArgumentParser, FileType
import os

class MATSimConfigFromTemplate:
    
    def __init__(self, template_config_file):
        self.tree = ET.parse(template_config_file)
        self.root_config = self.tree.getroot()

    def configure_threads(self, event_handler_threads, qsim_threads):
        param_event_handler_threads = self.root_config.find(".//*[@name='global']/*[@name='numberOfThreads']")
        param_event_handler_threads.attrib['value'] = str(event_handler_threads)

        param_qsim_threads = self.root_config.find(".//*[@name='qsim']/*[@name='numberOfThreads']")
        param_qsim_threads.attrib['value'] = str(qsim_threads)

    def configure_output_directory(self, output_dir):
        #output directory of matsim output is specified in the controler
        param_controler_outputDirectory = self.root_config.find(".//*[@name='controler']/*[@name='outputDirectory']")
        param_controler_outputDirectory.attrib['value'] = output_dir


    def configure_input_plans_file(self, input_plans_file):
        param_plans_inputPlansFile = self.root_config.find(".//*[@name='plans']/*[@name='inputPlansFile']")
        param_plans_inputPlansFile.attrib['value'] = input_plans_file

    def save_config_file(self, dest):
        with open(dest, 'wb') as out:
            out.write(b'<?xml version="1.0" ?>\n')
            out.write(b'<!DOCTYPE config SYSTEM "http://www.matsim.org/files/dtd/config_v2.dtd">\n')
            self.tree.write(out, xml_declaration=False)

def create_threads_list(cpu_cores):
    threads_list = []
    for x in (2**j for j in range(0, 10)):
        if (x >= cpu_cores): 
            if(x > cpu_cores):
                threads_list.append(cpu_cores)
                break
            threads_list.append(x)
            break
        threads_list.append(x)
    return threads_list


def main():
    parser = ArgumentParser(description="Generate multiple config.xml for parallel run testing")
    parser.add_argument('cpu_cores', type=int, help='Number of CPU cores')
    parser.add_argument('template_config_file', type=str, help='Path to the template config file')
    parser.add_argument('working_dir', type=str, help='Working directory to store generated files, read inputs, ...')
    
    parser.add_argument('matsim_input_plans_file', type=str, help='XML Plans file directory (in MATSim config file)')
    parser.add_argument('matsim_output_dir', type=str, help='Path to simulation output directory (in MATSim config file)')
    args = parser.parse_args()

    #creates output directory for generated configs
    #if not os.path.exists(args.output_dir):
    #    os.mkdir(args.output_dir)

    #get template config file
    config_gen = MATSimConfigFromTemplate(args.working_dir + args.template_config_file)
    
    #get input plans file name
    config_gen.configure_input_plans_file(args.matsim_input_plans_file)
    pop_file_name = args.matsim_input_plans_file.split('.')
    pop_file_name = pop_file_name[-3].split('/')
    pop_file_name = pop_file_name[-1]

    threads_list = create_threads_list(args.cpu_cores)
    pairs = [[eh, qsim] for eh in threads_list
                for qsim in threads_list]
    for p in pairs:
        config_gen.configure_threads(p[0], p[1])

        #set directory for matsim simulation output
        #config_gen.configure_output_directory(args.working_dir + args.matsim_output_dir + pop_file_name + "-eh" + str(p[0]) + "-qsim" + str(p[1]))
        config_gen.configure_output_directory(args.working_dir + "outputs/" + pop_file_name + "-eh" + str(p[0]) + "-qsim" + str(p[1]))

        #save the generated config file
        config_gen.save_config_file(args.working_dir + pop_file_name + "-config-prague-eh" + str(p[0]) + "-qsim" + str(p[1]) + ".xml")
    
if __name__ == "__main__":
    main()
