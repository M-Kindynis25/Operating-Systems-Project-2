// splitter.cpp
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <sys/times.h> 
#include "vector.hpp"

// Δομή Παραμέτρων
struct Parameters {
    int pipe_write_fd;
    char inputFile[256];
    char exclusionFile[256];
    int numOfBuilders;
    int startLine;
    int endLine;
    int idSplitter;
};

// Συνάρτηση για ανάλυση των ορισμάτων
Parameters parseArguments(int argc, char* argv[]);

// Συνάρτηση για την έυρεση της πίνακα λέξεων εξαιρέσεων
Vector<char*> vectorExclusionWords(const char* exclusionFile);

// Συνάρτηση για την επεξεργασία λέξεων (καθαρισμός και έλεγχος)
char* cleanWord(const char* word, Vector<char*>& exclusionList);

// Συνάρτηση κατακερματισμού για μια συμβολοσειρά
unsigned int hashFunction(const char* key, int size);


int main(int argc, char *argv[]) {
    struct tms tb1, tb2;

    // Αριθμός ticks ανά δευτερόλεπτο
    double ticspersec = static_cast<double>(sysconf(_SC_CLK_TCK));

    // Μέτρηση αρχικού χρόνου
    double t1 = static_cast<double>(times(&tb1));

    // Ανάλυση ορισμάτων
    Parameters params = parseArguments(argc, argv);

    // Δημιουργία λίστας εξαιρέσεων από το αρχείο εξαιρέσεων
    Vector<char*> exclusionList = vectorExclusionWords(params.exclusionFile);

    // Δημιουργία λίστας file descriptors για named pipes
    Vector<int> builderPipeDescriptors;
    for (int j = 0; j < params.numOfBuilders; j++) { 
        // Δημιουργία του ονόματος του named pipe
        char fifo_path[50];
        sprintf(fifo_path, "fifo_splitter%d_builder%d", params.idSplitter, j);

        // Άνοιγμα του named pipe για εγγραφή
        int fd = open(fifo_path, O_WRONLY);
        if (fd == -1) {     // Έλεγχος αποτυχίας ανοίγματος
            std::perror("open");
            return 2;
        }

        builderPipeDescriptors.push_back(fd);   // Προσθήκη του file descriptor στον vector
    }

    

    // Άνοιγμα του αρχείου εισόδου για ανάγνωση
    int file_fd = open(params.inputFile, O_RDONLY);
    if (file_fd == -1) {    // Έλεγχος αποτυχίας ανοίγματος
        std::perror("open input file");
        for (size_t i = 0; i < builderPipeDescriptors.get_size(); i++) close(builderPipeDescriptors[i]);
        return 2;
    }

    // Μετατροπή του file descriptor σε FILE* για χρήση με fgets
    FILE* file = fdopen(file_fd, "r");
    if (file == NULL) {     // Έλεγχος αποτυχίας μετατροπής
        std::perror("fdopen");
        for (size_t i = 0; i < builderPipeDescriptors.get_size(); i++) close(builderPipeDescriptors[i]);
        close(file_fd);
        return 2;
    }

    const size_t buffer_size = 1024;
    char buffer[buffer_size];

    int  line = 0;  // Μετρητής γραμμών
    while (fgets(buffer, buffer_size, file) != NULL) {  // Ανάγνωση γραμμών από το αρχείο
        if (line >= params.startLine && line <= params.endLine) {   // Επεξεργασία μόνο των γραμμών εντός του εύρους
            // Διαχωρισμός της γραμμής σε λέξεις
            char* token = strtok(buffer, " \t\n");
            while (token != NULL) {
                // Καθαρισμός της λέξης από σημεία στίξης και εξαιρέσεις
                char* cleanToken = cleanWord(token, exclusionList);

                // Έλεγχος αν το cleanToken είναι nullptr ή κενή αλυσίδα
                if (cleanToken == nullptr) {
                    token = strtok(NULL, " \t\n"); // Επόμενη λέξη
                    continue;
                }
                
                // Υπολογισμός του builder που θα λάβει τη λέξη
                int builderIndex = hashFunction(cleanToken, params.numOfBuilders);
                if (builderIndex < 0 || builderIndex >= params.numOfBuilders) {     // Έλεγχος εγκυρότητας του index
                    std::perror("hashFunction");
                    delete[] cleanToken;
                    return 2;
                } 

                // Εγγραφή της λέξης στο αντίστοιχο pipe
                ssize_t bytes_written = write(builderPipeDescriptors[builderIndex], cleanToken, strlen(cleanToken));
                if (bytes_written == -1) {      // Έλεγχος αποτυχίας εγγραφής
                    std::perror("write");
                    fclose(file);
                    for (size_t i = 0; i < builderPipeDescriptors.get_size(); i++) close(builderPipeDescriptors[i]);
                    return 2;
                }

                // Γράψιμο νέας γραμμής για διαχωρισμό λέξεων
                write(builderPipeDescriptors[builderIndex], "\n", 1);

                // Απελευθέρωση της μνήμης που δεσμεύτηκε από το cleanWord
                delete[] cleanToken;

                token = strtok(NULL, " \t\n"); // Επόμενη λέξη
            }
        }
        if (line >= params.endLine)  break;     // Τερματισμός όταν περάσουμε το endLine
        line ++;
    }

    // Κλείνει το αρχείο εισόδου
    fclose(file); 
    // Κλείσιμο όλων των named pipes προς τους builders
    for (size_t i = 0; i < builderPipeDescriptors.get_size(); i++) close(builderPipeDescriptors[i]);
    // Απελευθέρωση της μνήμης για τη λίστα εξαιρέσεων
    for (size_t i = 0; i < exclusionList.get_size(); ++i) {
        delete[] exclusionList[i];
    }

    // Μέτρηση τελικού χρόνου
    double t2 = static_cast<double>(times(&tb2));

    // Υπολογισμός CPU χρόνου
    double cpu_time = static_cast<double>((tb2.tms_utime + tb2.tms_stime) - (tb1.tms_utime + tb1.tms_stime));

    // Ειδοποίηση της ρίζας ότι ο splitter ολοκλήρωσε την εργασία του
    char write_path[70];
    snprintf(write_path, sizeof(write_path), "SplitterDone-%d-%f-%f", params.idSplitter, (t2 - t1) / ticspersec, cpu_time / ticspersec); 
    if (write(params.pipe_write_fd, write_path, strlen(write_path) + 1) < 0) {
        std::perror("Error writing SplitterDone message to pipe.");
        return 2;
    }
    close(params.pipe_write_fd); // Κλείνουμε το pipe επικοινωνίας με τη ρίζα

    return 0;
}

Parameters parseArguments(int argc, char* argv[]) {
    Parameters params = {-1, "", "", 0, -1, -1, -1};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            params.pipe_write_fd = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-id") == 0 && i + 1 < argc) { 
            params.idSplitter = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            strncpy(params.inputFile, argv[i + 1], 255);
            params.inputFile[255] = '\0';
            i++;
        } else if (strcmp(argv[i], "-e") == 0 && i + 1 < argc) {
            strncpy(params.exclusionFile, argv[i + 1], 255);
            params.exclusionFile[255] = '\0';
            i++;
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            params.numOfBuilders = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-sL") == 0 && i + 1 < argc) { 
            params.startLine = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-eL") == 0 && i + 1 < argc) { 
            params.endLine = std::atoi(argv[i + 1]);
            i++;
        }
    }

    if (strlen(params.inputFile) == 0 || strlen(params.exclusionFile) == 0 || params.numOfBuilders <= 0 ||
        params.startLine < 0 || params.endLine < params.startLine || params.idSplitter < 0) {
        std::cerr << "Usage: ./splitter -i inputfile -e exclusionfile -m numOfBuilders -sL startLine -eL endLine" << std::endl;
        std::exit(1);
    }

    return params;
}

Vector<char*> vectorExclusionWords(const char* exclusionFile) {
    Vector<char*> exclusionWords;
    // Άνοιγμα του αρχείου εξαιρέσεων με open
    int file_fd = open(exclusionFile, O_RDONLY);
    if (file_fd == -1) {  // Έλεγχος αποτυχίας ανοίγματος
        std::perror("open exclusion file");
        return exclusionWords;
    }

    // Μετατροπή του file descriptor σε FILE* για χρήση με fgets
    FILE* file = fdopen(file_fd, "r");
    if (file == NULL) {  // Έλεγχος αποτυχίας μετατροπής
        std::perror("fdopen");
        close(file_fd);
        return exclusionWords;
    }

    const size_t buffer_size = 256;
    char buffer[buffer_size];

    // Ανάγνωση του αρχείου γραμμή προς γραμμή
    while (fgets(buffer, buffer_size, file) != NULL) {
        // Αφαίρεση του χαρακτήρα νέας γραμμής στο τέλος (αν υπάρχει)
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len - 1] == '\n') {
            buffer[len - 1] = '\0';
        }

        // Διαχωρισμός της γραμμής σε λέξεις
        char* token = strtok(buffer, " \t");
        while (token != NULL) {
            // Δημιουργία αντιγράφου της λέξης
            char* word = new char[strlen(token) + 1];
            std::strcpy(word, token);

            exclusionWords.push_back(word);  // Προσθήκη της λέξης στον Vector

            token = strtok(NULL, " \t");    // Επόμενο toke
        }
    }

    fclose(file); // Κλείσιμο του αρχείου (κλείνει και το file_fd)
    return exclusionWords;
}

char* cleanWord(const char* word, Vector<char*>& exclusionList) {
    // Υπολογισμός μήκους της λέξης
    int length = std::strlen(word);
    
    // Δημιουργία buffer για την καθαρισμένη λέξη
    char* cleanedWord = new char[length + 1];
    int index = 0;

    // Αφαίρεση σημείων στίξης, συμβόλων και ψηφίων
    for (int i = 0; i < length; ++i) {
        if (std::isalpha(static_cast<unsigned char>(word[i]))) {        // Ελέγχει αν ο χαρακτήρας είναι γράμμα
            cleanedWord[index++] = std::tolower(static_cast<unsigned char>(word[i]));  // Μετατροπή σε πεζά
        }
    }
    cleanedWord[index] = '\0';      // Τερματισμός της καθαρισμένης συμβολοσειράς

    // Έλεγχος αν η καθαρισμένη λέξη είναι κενή ή αν εχει ένα χαρακτήρα
    if (index == 0 || index == 1) {
        delete[] cleanedWord;       // Απελευθέρωση μνήμης αν δεν υπάρχει καθαρισμένη λέξη
        return nullptr;
    }

    // Έλεγχος αν η καθαρισμένη λέξη βρίσκεται στη λίστα αποκλεισμού
    for (size_t i = 0; i < exclusionList.get_size(); ++i) {
        if (std::strcmp(cleanedWord, exclusionList[i]) == 0) {      // Σύγκριση με κάθε λέξη της exclusion list
            delete[] cleanedWord;       // Αν βρεθεί στη λίστα, απελευθέρωση μνήμης
            return nullptr;
        }
    }

    // Επιστροφή της καθαρισμένης λέξης
    return cleanedWord;
}

unsigned int hashFunction(const char* key, int size) {
    unsigned int hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % size;
}
