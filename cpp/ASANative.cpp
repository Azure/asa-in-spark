// #include "CoreCLRHost.hpp"
#include "utils.hpp"
#include "build/com_microsoft_ASANative.h"
#include <stdexcept>
#include <sstream>
#include <wchar.h>
#include <iostream>
#include <stdio.h>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <dlfcn.h>
#endif

#if defined(__APPLE__)
  std::string coreClrDll = "libcoreclr.dylib";
#else
  std::string coreClrDll = "libcoreclr.so";
#endif

// TODO: remove me
#if not defined PATH_MAX
  #include <stdio.h>
  #define PATH_MAX FILENAME_MAX
#endif


// some JNI helper
class StringGuard {
    JNIEnv* _env;
    jstring _source;
    const char* _cstr;
public:
    StringGuard(JNIEnv *env, jstring source) : _env(env), _source(source), _cstr(nullptr) {
        _cstr = _env->GetStringUTFChars(source, 0);
    }

    ~StringGuard() {
        if (_cstr) {
            _env->ReleaseStringUTFChars(_source, _cstr);
            _env->DeleteLocalRef(_source);
        }
    }

    const char* c_str() { return _cstr; }
};  

class StringUniGuard {
    JNIEnv* _env;
    jstring _source;
    const jchar* _cstr;
    jsize _length;
public:
    StringUniGuard(JNIEnv *env, jstring source) : _env(env), _source(source), _cstr(nullptr) {
        _cstr = _env->GetStringChars(source, nullptr);
        _length = _env->GetStringLength(source);
    }

    ~StringUniGuard() {
        if (_cstr) {
            _env->ReleaseStringChars(_source, _cstr);
            _env->DeleteLocalRef(_source);
        }
    }

    jsize length() { return _length; }

    const jchar* c_str() { return _cstr; }
};  

typedef void* (csharp_createASAHost_t)(const char16_t* sqlPtr, int sqlLen);
typedef void (csharp_deleteASAHost_t)(void* ptr);

typedef void (csharp_getOutputSchema_t)(void* ptr, const char16_t* sqlPtr, int sqlLen);
typedef void (csharp_registerArrowMemory_t)(void* ptr, jbyte* outputBuffer, int outputBufferLength, jbyte* inputBuffer, int inputBufferLength);
typedef void (csharp_runDotnetSpark_t)(const char16_t* localCsvNamePtr, int localCsvNameLen);
typedef long (csharp_pushRecord_t)(void* ptr);
typedef int (csharp_pushComplete_t)(void* ptr);
typedef void (csharp_nextOutputRecord_t)(void* ptr);
typedef void (csharp_stringFree_t)(const char16_t*);

class ASAHostException : public std::runtime_error {
public:
    ASAHostException(const char* what) : std::runtime_error(what) {
    }
};

class ASAHost;

class CLRHost {
  void* hostHandle;
  unsigned int domainId;
  coreclrShutdownFunction* coreclr_shutdown_f;
  coreclrCreateDelegateFunction* coreclr_create_delegate_f;
  void* libraryPtr;


    void* resolveFunctionPtr(const char* name) {
        // std::cout << "resolveFunctionPtr " << name << std::endl;
        void* f = 
        #ifdef _WIN32
          GetProcAddress((HINSTANCE)libraryPtr , name);
        #else
          dlsym(libraryPtr, name);
        #endif

        if (!f) {
            std::stringstream msg;
            msg << "resolveFunctionPtr was unable to resolve " << name;
            throw ASAHostException(msg.str().c_str());
        }

        return f;
    }


public:
    CLRHost() : hostHandle(NULL), domainId(0), libraryPtr(nullptr) {
    }

    void freeCLR() {
        int status = coreclr_shutdown_f(hostHandle, domainId);

        if(status < 0) {
            std::stringstream msg;
            msg << "coreclr_shutdown status: 0x" << std::hex << status;
            throw std::runtime_error(msg.str());
        }
    }

    ~CLRHost() {
        if (libraryPtr) { 
#ifdef _WIN32
            FreeLibrary((HINSTANCE)libraryPtr);
#else
            dlclose(libraryPtr);
#endif
        }
    }

    void initialize(std::string tempDir) {
        std::string currentExeAbsolutePath = tempDir; 
        std::string clrFilesAbsolutePath = tempDir + "/clr"; 
        std::string managedAssemblyAbsoluteDir = tempDir + "/binding"; 

        std::string coreClrDllPath = clrFilesAbsolutePath + "/" + coreClrDll;

        if(coreClrDllPath.size() >= PATH_MAX)
            throw std::invalid_argument("Path to libcoreclr.so too long!");

        // std::cout << "native.load library: " << coreClrDllPath << std::endl;
        // get library handle
        libraryPtr = 
            #ifdef _WIN32
                LoadLibrary(coreClrDllPath.c_str() );
            #else
                dlopen(coreClrDllPath.c_str(), RTLD_NOW | RTLD_LOCAL );
            #endif
        if (!libraryPtr) {
            std::stringstream msg;
            msg << "Cannot find " << coreClrDll << "Path that was searched: "
                << coreClrDllPath;
            throw ASAHostException(msg.str().c_str());
        }
        // std::cout << "native.load library succcess"<< std::endl;

        std::string nativeDllSearchDirs = managedAssemblyAbsoluteDir + ":" + clrFilesAbsolutePath;

        std::string tpaList;
        AddFilesFromDirectoryToTpaList(clrFilesAbsolutePath, tpaList);

        // std::cout << "TRUSTED_PLATFORM_ASSEMBLIES " << tpaList << std::endl;
        // std::cout << "APP_PATHS: " << managedAssemblyAbsoluteDir << std::endl;

        auto coreclr_initialize = (coreclrInitializeFunction*)resolveFunctionPtr("coreclr_initialize");
        coreclr_shutdown_f = (coreclrShutdownFunction*)resolveFunctionPtr("coreclr_shutdown");
        coreclr_create_delegate_f = (coreclrCreateDelegateFunction*)resolveFunctionPtr("coreclr_create_delegate");

        const char *propertyKeys[] = {
            "TRUSTED_PLATFORM_ASSEMBLIES",
            "APP_PATHS",
            "APP_NI_PATHS",
            "NATIVE_DLL_SEARCH_DIRECTORIES",
            "AppDomainCompatSwitch"
        };

        const char *propertyValues[] = {
            tpaList.c_str(),
            managedAssemblyAbsoluteDir.c_str(),
            managedAssemblyAbsoluteDir.c_str(),
            nativeDllSearchDirs.c_str(),
            "UseLatestBehaviorWhenTFMNotSpecified"
        };

        // initialize coreclr
        int status = coreclr_initialize(
            currentExeAbsolutePath.c_str(),
            "simpleCoreCLRHost",
            sizeof(propertyKeys) / sizeof(propertyKeys[0]),
            propertyKeys,
            propertyValues,
            &hostHandle,
            &domainId
            );

        if (status < 0) {
            std::stringstream msg;
            msg << "coreclr_initialize status: 0x" << std::hex << status;
            throw std::runtime_error(msg.str());
        }
    }

    template<typename T>
    T* getCSharpEntryPoint(const char* entrypointtype, const char* entrypointname) {
        T* csharp_ptr;

        // create delegate to our entry point
        int status = coreclr_create_delegate_f(
                hostHandle,
                domainId,
                "Managed", // Assembly spec
                entrypointtype, // class name
                entrypointname, // method name
                reinterpret_cast<void**>(&csharp_ptr)
            );

        if (status < 0) {
            std::stringstream msg;
            msg << "getCSharpEntryPoint failed. status: 0x" << std::hex << status;
            throw std::runtime_error(msg.str());
        }

        return csharp_ptr;
    }


    friend class ASAHost;
};

// main dispatcher class
class ASAHost {
  CLRHost* _clrHost;
  csharp_createASAHost_t* createASAHost;
  csharp_deleteASAHost_t* deleteASAHost;
  csharp_pushRecord_t* pushRecord_f;
  csharp_pushComplete_t* pushComplete_f;
  csharp_nextOutputRecord_t* nextOutputRecord_f;
  csharp_stringFree_t* stringFree_f;
  csharp_getOutputSchema_t* getOutputSchema_f;
  csharp_registerArrowMemory_t* registerArrowMemory_f;

  void* csharp_ASAHost;

    public:

    ASAHost(CLRHost* clrHost) : _clrHost(clrHost), createASAHost(nullptr), deleteASAHost(nullptr) {
    }

    void initializeCLR(std::string tempDir,
        const jchar* sql, jsize sqlLen) {

        // boilerplate code from: https://docs.microsoft.com/en-us/dotnet/core/tutorials/netcore-hosting

        createASAHost = _clrHost->getCSharpEntryPoint<csharp_createASAHost_t>("ASAHost", "createASAHost");
        deleteASAHost = _clrHost->getCSharpEntryPoint<csharp_deleteASAHost_t>("ASAHost", "deleteASAHost");

        // forward get/set
        stringFree_f = _clrHost->getCSharpEntryPoint<csharp_stringFree_t>("ASAHost", "stringFree");

        pushComplete_f = _clrHost->getCSharpEntryPoint<csharp_pushComplete_t>("ASAHost", "pushComplete");
        pushRecord_f = _clrHost->getCSharpEntryPoint<csharp_pushRecord_t>("ASAHost", "pushRecord");
        nextOutputRecord_f = _clrHost->getCSharpEntryPoint<csharp_nextOutputRecord_t>("ASAHost", "nextOutputRecord");
        getOutputSchema_f = _clrHost->getCSharpEntryPoint<csharp_getOutputSchema_t>("ASAHost", "getOutputSchema");
        registerArrowMemory_f = _clrHost->getCSharpEntryPoint<csharp_registerArrowMemory_t>("ASAHost", "registerArrowMemory");

        csharp_ASAHost = (*createASAHost)((char16_t*)sql, sqlLen);
    } 

    void freeCLR() {
    }

    int pushRecord() { return (*pushRecord_f)(csharp_ASAHost); }
    int pushComplete() { return (*pushComplete_f)(csharp_ASAHost); }
    void nextOutputRecord() { (*nextOutputRecord_f)(csharp_ASAHost); }
    void registerArrowMemory(jbyte* outputBuffer, int outputBufferLength, jbyte* inputBuffer, int inputBufferLength)
        {(*registerArrowMemory_f)(csharp_ASAHost, outputBuffer, outputBufferLength, inputBuffer, inputBufferLength); }
    void getOutputSchema(const char16_t* sqlPtr, int sqlLen) {(*getOutputSchema_f)(csharp_ASAHost, sqlPtr, sqlLen);}
};

JNIEXPORT jlong JNICALL Java_com_microsoft_ASANative_startCLR
  (JNIEnv *env, jclass, jstring tempDir) {
    try
    {
        StringGuard tempDirStr(env, tempDir);

        auto clr = new CLRHost();
        clr->initialize(tempDirStr.c_str());

        return (jlong)clr;
    }
    catch(std::exception& e)
    {
        env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); 
    }
}

JNIEXPORT jlong JNICALL Java_com_microsoft_ASANative_startASA
  (JNIEnv *env, jclass cls, jlong clrPtr, jstring tempDir, jstring sql) {
    ASAHost* host = nullptr;
    try
    {
        StringGuard tempDirStr(env, tempDir);
        StringUniGuard sqlStr(env, sql);

        host = new ASAHost((CLRHost*)clrPtr);
        host->initializeCLR(
            tempDirStr.c_str(),
            sqlStr.c_str(), sqlStr.length());

        return (jlong)host;
    }
    catch(std::exception& e)
    {
        if (host)
            delete host;

        // forward exception
        env->ThrowNew(env->FindClass("java/lang/Exception"), e.what());
    }
}

JNIEXPORT void JNICALL Java_com_microsoft_ASANative_getOutputSchema
  (JNIEnv *env, jclass cls, jlong ptr, jstring sql) {
    try {
        StringUniGuard sqlStr(env, sql);
        
        ((ASAHost*)ptr)->getOutputSchema((char16_t*)sqlStr.c_str(), sqlStr.length());
    } 
    catch(std::exception& e) 
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}

JNIEXPORT void JNICALL Java_com_microsoft_ASANative_registerArrowMemory
  (JNIEnv *env, jclass cls, jlong ptr, jobject outputBuffer, jint outputBufferLength,
   jobject inputBuffer, jint inputBufferLength)
{
    try {
        jbyte* bbuf_out;
        jbyte* bbuf_in;

        bbuf_out = (jbyte*)(env->GetDirectBufferAddress(outputBuffer));
        bbuf_in = (jbyte*)(env->GetDirectBufferAddress(inputBuffer));

        ((ASAHost*)ptr)->registerArrowMemory(bbuf_out, outputBufferLength, bbuf_in, inputBufferLength);
    }
    catch (std::exception& e)
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}

JNIEXPORT jint JNICALL Java_com_microsoft_ASANative_pushRecord
  (JNIEnv *env, jclass, jlong ptr) {
    try { return ((ASAHost*)ptr)->pushRecord(); }
    catch(std::exception& e)
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}

JNIEXPORT jint JNICALL Java_com_microsoft_ASANative_pushComplete
  (JNIEnv *env, jclass, jlong ptr) {
    try { return ((ASAHost*)ptr)->pushComplete(); }
    catch(std::exception& e)
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}

JNIEXPORT void JNICALL Java_com_microsoft_ASANative_nextOutputRecord
  (JNIEnv *env, jclass, jlong ptr) {
    try { ((ASAHost*)ptr)->nextOutputRecord(); }
    catch(std::exception& e)
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}

JNIEXPORT void JNICALL Java_com_microsoft_ASANative_stopASA
  (JNIEnv *env, jclass, jlong ptr) {
    try {
        ASAHost* host = (ASAHost*)ptr;
        host->freeCLR();
        delete host;
    }
    catch(std::exception& e)
    { env->ThrowNew(env->FindClass("java/lang/Exception"), e.what()); }
}
