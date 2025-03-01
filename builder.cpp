// builder.cpp
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <sys/times.h> 
#include "vector.hpp"

// Δομή Παραμέτρων
struct Parameters {
    int pipe_write_fd;
    int builderID;
    int numOfSplitters;
    int topK;
};

// Δομή που χρησιμοποιείται για την αποθήκευση μιας λέξης και του πλήθους εμφάνισής της
struct WordCount {
    char word[64];
    int count;
};

// Συνάρτηση για ανάλυση των ορισμάτων
Parameters parseArguments(int argc, char* argv[]);

// Συνάρτηση που μετατρέπει έναν ακέραιο σε συμβολοσειρά
const char* intToStr(int number);
 
// Ελέγχει αν η λέξη υπάρχει ήδη στον vec.
// Αν υπάρχει, αυξάνει το count της αντίστοιχης λέξης.
// Αν δεν υπάρχει, δημιουργεί ένα νέο WordCount αντικείμενο, το προσθέτει στον vec και θέτει το count σε 1.
void processWord(char buffer[64], Vector<WordCount>& vec);

// Πρότυπη συνάρτηση για την ταξινόμηση ενός Vector σε σχεση με το count
template <typename T, typename Compare>
void vector_sort(Vector<T>& vec, Compare comp);


int main(int argc, char* argv[]) {
    struct tms tb1, tb2;

    // Αριθμός ticks ανά δευτερόλεπτο
    double ticspersec = static_cast<double>(sysconf(_SC_CLK_TCK));

    // Μέτρηση αρχικού χρόνου
    double t1 = static_cast<double>(times(&tb1));

    // Ανάλυση ορισμάτων
    Parameters params = parseArguments(argc, argv);

    Vector<int> splitterPipeDescriptors;
    for (int i = 0; i < params.numOfSplitters; i++) { 
        // Δημιουργία ονόματος για το named pipe
        char fifo_path[50];
        sprintf(fifo_path, "fifo_splitter%d_builder%d", i, params.builderID);

        // Άνοιγμα του named pipe για ανάγνωση σε blocking mode
        int fd = open(fifo_path, O_RDONLY);
        if (fd == -1) {
            std::perror("open");
            std::exit(EXIT_FAILURE);
        }
        splitterPipeDescriptors.push_back(fd);      // Αποθήκευση του file descriptor στον vector
    }

    // Δομή για αποθήκευση λέξεων και μετρήσεων
    Vector<WordCount> wordVector;

    const size_t buffer_size = 1024;
    char buffer[buffer_size];

    // Δείκτες για ενεργά FIFOs
    Vector<bool> activeFds;
    for (int i = 0; i < params.numOfSplitters; i++) activeFds.push_back(true);
    
    int activeCount = params.numOfSplitters;    // Αριθμός ενεργών FIFOs

    // Βρόχος ανάγνωσης δεδομένων μέχρι να κλείσουν όλα τα FIFOs
    while (activeCount > 0) {
        for (int i = 0; i < params.numOfSplitters; i++) { 
            if (activeFds[i]) {     // Ελέγχει αν το FIFO είναι ακόμα ενεργό
                ssize_t bytes_read = read(splitterPipeDescriptors[i], buffer, buffer_size - 1);
                
                if (bytes_read == -1) {     // Έλεγχος σφαλμάτων ανάγνωσης
                    std::perror("read");
                    return 2;
                }
                if (bytes_read == 0) {      // Ανάγνωση EOF, το FIFO είναι πλέον κλειστό
                    activeFds[i] = false;
                    activeCount--;
                    break; 
                }

                buffer[bytes_read] = '\0';  // Διασφάλιση null-τερματισμού για ασφαλή επεξεργασία ως string

                // Διαχωρισμός λέξεων από τον buffer
                char* token = strtok(buffer, " \n");
                while (token != NULL) {
                    processWord(token, wordVector);  // Καταμέτρηση λέξης στον wordVector
                    token = strtok(NULL, " \n");     // Επόμενο token
                }
            }
        }
    }

    // Ταξινόμηση του wordVector με βάση το count των λέξεων σε φθίνουσα σειρά
    vector_sort(wordVector, [](const WordCount& a, const WordCount& b) {
        return a.count > b.count;
    });

    // Δημιουργία διαδρομής για το named pipe που θα συνδέει τον builder με το laxen
    char fifo_path[50];
    sprintf(fifo_path, "fifo_builder%d_laxen", params.builderID);
    // Άνοιγμα του named pipe για εγγραφή
    int outputFifoFd = open(fifo_path, O_WRONLY);
    if (outputFifoFd == -1) {
        std::perror("open");
        return EXIT_FAILURE;
    }

    // Εγγραφή των topK λέξεων στο named pipe
    for (size_t i = 0; i < static_cast<size_t>(params.topK) && i < wordVector.get_size(); i++) {
        char write_path[70];
        snprintf(write_path, sizeof(write_path), "%s-%d", wordVector[i].word, wordVector[i].count);   // Δημιουργία εγγραφής "λέξη-αριθμός"  

        if (strlen(write_path) < 3) break;  // Έλεγχος για μη έγκυρες εγγραφές

        ssize_t bytes_written = write(outputFifoFd, write_path, strlen(write_path));     // Εγγραφή στο FIFO
        if (bytes_written == -1) {   // Έλεγχος σφαλμάτων κατά την εγγραφή
            std::perror("write");
            close(outputFifoFd);
            for (size_t i = 0; i < splitterPipeDescriptors.get_size(); i++) close(splitterPipeDescriptors[i]);     // Κλείσιμο όλων των splitter pipes
            return EXIT_FAILURE;
        }

        // Γράψιμο χαρακτήρα νέας γραμμής για διαχωρισμό εγγραφών
        write(outputFifoFd, "\n", 1);
    }
    // Κλείσιμο του output FIFO μετά την ολοκλήρωση
    close(outputFifoFd);

    // Κλείσιμο όλων των pipes προς τους splitters
    for (size_t i = 0; i < splitterPipeDescriptors.get_size(); i++) close(splitterPipeDescriptors[i]);

    // Μέτρηση τελικού χρόνου
    double t2 = static_cast<double>(times(&tb2));

    // Υπολογισμός CPU χρόνου
    double cpu_time = static_cast<double>((tb2.tms_utime + tb2.tms_stime) - (tb1.tms_utime + tb1.tms_stime));

    // Ειδοποίηση της ρίζας ότι ο builder ολοκλήρωσε την εργασία του
    char write_path[70];
    snprintf(write_path, sizeof(write_path), "BuilderDonee-%d-%f-%f", params.builderID, (t2 - t1) / ticspersec, cpu_time / ticspersec); 
    if (write(params.pipe_write_fd, write_path, std::strlen(write_path) + 1) < 0) {
        std::cerr << "Error writing BuilderDone message to pipe." << std::endl;
    }
    close(params.pipe_write_fd); // Κλείσιμο του pipe επικοινωνίας

    return 0;
}


Parameters parseArguments(int argc, char* argv[]) {
    Parameters params = {-1, -1, 0, 0};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            params.pipe_write_fd = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-id") == 0 && i + 1 < argc) { 
            params.builderID = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) { 
            params.topK = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) { 
            params.numOfSplitters = std::atoi(argv[i + 1]);
            i++;
        }
    }

    if (params.pipe_write_fd < 0 || params.builderID < 0 || params.topK <= 0  || params.numOfSplitters <= 0) {
        std::cerr << "Usage: ./builder -p pipe_write_fd -id builderID -t topK -l numOfSplitters" << std::endl;
        std::exit(1);
    }

    return params;
}

const char* intToStr(int number) {
    static char buffers[10][20];
    static int index = 0;

    char* str = buffers[index];
    index = (index + 1) % 10;

    sprintf(str, "%d", number);
    return str;
}

void processWord(char buffer[64], Vector<WordCount>& vec) {
    // Αναζήτηση της λέξης στον vector
    for (size_t i = 0; i < vec.get_size(); ++i) {
        if (strcmp(vec[i].word, buffer) == 0) {
            // Αν η λέξη βρέθηκε, αύξησε τον μετρητή
            vec[i].count++;
            return;
        }
    }

    // Αν η λέξη δεν βρέθηκε, πρόσθεσέ την με αρχική τιμή count = 1
    WordCount newWord;
    strcpy(newWord.word, buffer);
    newWord.count = 1;
    vec.push_back(newWord);
}

template <typename T, typename Compare>
void vector_sort(Vector<T>& vec, Compare comp) {
    size_t n = vec.get_size();
    // Εφαρμογή αλγορίθμου ταξινόμησης
    for (size_t i = 0; i < n - 1; ++i) {
        for (size_t j = 0; j < n - i - 1; ++j) {
            // Χρήση της συνάρτησης σύγκρισης comp για να αποφασιστεί η σειρά
            if (comp(vec[j + 1], vec[j])) {
                // Ανταλλαγή των στοιχείων vec[j] και vec[j + 1]
                T temp = vec[j];
                vec[j] = vec[j + 1];
                vec[j + 1] = temp;
            }
        }
    }
}
